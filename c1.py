import logging
from policy.guard import guard_allows, note_trade_event
# strategies/c1.py
import os, math
from typing import List, Dict
try:
    import br_router as br
except Exception:
    from strategies import br_router as br
from strategies import utils_volatility as uv

STRAT = "c1"
VER   = os.getenv("C1_VER", "v2.0")

def _ema(vals, n):
    if n <= 1 or len(vals) < n: return None
    k = 2.0 / (n + 1.0)
    ema = vals[0]
    for v in vals[1:]:
        ema = v * k + ema * (1.0 - k)
    return ema

def _vwap(prices: List[float], vols: List[float]) -> float:
    if not prices or not vols or len(prices) != len(vols): return None
    num = sum(p*v for p, v in zip(prices, vols))
    den = sum(vols)
    return num / max(den, 1e-12)

def _pos_for(symbol: str):
    sym = symbol.replace("/", "")
    pos = br.list_positions() or []
    for p in pos:
        if p.get("symbol","").replace("/","") == symbol.replace("/",""):
            return float(p.get("qty", 0.0) or 0.0), float(p.get("avg_entry_price", 0.0) or 0.0)
    return 0.0, 0.0

def run_scan(symbols: List[str], timeframe: str, limit: int, notional: float, dry: bool, raw: Dict) -> None:
    # Parameters
    N_EMA      = int(raw.get("ema_n", 20))
    VWAP_PULL  = float(raw.get("vwap_pull", 0.002))
    MIN_VOL    = float(raw.get("min_vol", 0.0))

    for sym in symbols:
        bars = br.get_bars(sym, timeframe, limit) or []
        if len(bars) < max(20, N_EMA): 
            continue

        closes = [b["c"] for b in bars]
        vols   = [b["v"] for b in bars]
        if sum(vols[-10:]) < MIN_VOL:
            continue

        last      = closes[-1]
        ema_n     = _ema(closes[-N_EMA:], N_EMA)
        vwap_val  = _vwap(closes[-N_EMA:], vols[-N_EMA:])

        if vwap_val is None or ema_n is None:
            continue

        have, avg_px = _pos_for(sym)
        pull = (vwap_val - last) / max(vwap_val, 1e-9)
        symclean = sym.replace("/", "").lower()

        if have <= 1e-12 and pull >= VWAP_PULL and last >= ema_n:
            cid = f"{STRAT}-{VER}-buy-{symclean}"
            br.place_order(sym, "buy", notional, cid)
            return

        if have > 1e-12 and (last >= vwap_val or last < ema_n):
            cid = f"{STRAT}-{VER}-sell-{symclean}"
            br.place_order(sym, "sell", notional, cid)
            return

logger = logging.getLogger(__name__)


def guarded_place(symbol, expected_move_pct=None, atr_pct=None):
    ok, reason = guard_allows(strategy="c1", symbol=symbol, expected_move_pct=expected_move_pct, atr_pct=atr_pct)
    if not ok:
        logger.info(f"[guard] c1 blocked {symbol}: {reason}")
        return False
    return True


def policy_claim(symbol):
    try:
        note_trade_event("claim", strategy="c1", symbol=symbol)
    except Exception as e:
        logger.debug(f"[policy] c1 claim failed: {e}")

def policy_release(symbol):
    try:
        note_trade_event("release", strategy="c1", symbol=symbol)
    except Exception as e:
        logger.debug(f"[policy] c1 release failed: {e}")
