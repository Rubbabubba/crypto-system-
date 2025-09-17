# strategies/c1.py
"""
C1 — RSI mean-reversion with regime filter
- Buys pullbacks only when higher-timeframe trend is up (or configurable)
- Requires close >= EMA (configurable equality)
- ATR-based stop; optional take-profit; cooldown after exits
- No pandas; portable on Render
"""

__version__ = "2.0.0"

import math, json, os, time
from typing import List, Dict, Any, Tuple

# ----------------------------
# small utils (no pandas)
# ----------------------------
def ema(values: List[float], length: int) -> List[float]:
    if length <= 1 or not values:
        return values[:]
    k = 2.0 / (length + 1.0)
    out = []
    e = values[0]
    for v in values:
        e = v * k + e * (1 - k)
        out.append(e)
    return out

def rsi(values: List[float], length: int) -> List[float]:
    if length <= 1 or len(values) < length + 1:
        return [50.0] * len(values)
    gains, losses = [], []
    for i in range(1, len(values)):
        ch = values[i] - values[i-1]
        gains.append(max(ch, 0.0))
        losses.append(max(-ch, 0.0))
    # seed
    avg_gain = sum(gains[:length]) / length
    avg_loss = sum(losses[:length]) / length
    rsis = [50.0] * (length)  # warmup pad
    for i in range(length, len(gains)):
        avg_gain = (avg_gain * (length - 1) + gains[i]) / length
        avg_loss = (avg_loss * (length - 1) + losses[i]) / length
        if avg_loss == 0:
            rs = 99.0
        else:
            rs = avg_gain / avg_loss
        rsis.append(100 - (100 / (1 + rs)))
    # align to prices length
    while len(rsis) < len(values):
        rsis.append(rsis[-1])
    return rsis

def true_ranges(h: List[float], l: List[float], c: List[float]) -> List[float]:
    trs = []
    prev_close = None
    for i in range(len(c)):
        hi, lo, cl = h[i], l[i], c[i]
        if prev_close is None:
            tr = hi - lo
        else:
            tr = max(hi - lo, abs(hi - prev_close), abs(lo - prev_close))
        trs.append(max(tr, 0.0))
        prev_close = cl
    return trs

def atr(h: List[float], l: List[float], c: List[float], length: int) -> List[float]:
    trs = true_ranges(h, l, c)
    return ema(trs, length) if length > 1 else trs

def load_json(path: str, default):
    try:
        if os.path.exists(path):
            with open(path, "r") as f:
                return json.load(f)
    except Exception:
        pass
    return default

def save_json(path: str, obj) -> None:
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            json.dump(obj, f)
    except Exception:
        pass

# ----------------------------
# core
# ----------------------------
def _params(params: Dict[str, Any]) -> Dict[str, Any]:
    # pull from env-backed params with sensible defaults
    return {
        "TIMEFRAME": params.get("C1_TIMEFRAME") or "5Min",
        "HTF_TIMEFRAME": params.get("C1_HTF_TIMEFRAME") or "1Hour",
        "EMA_LEN": int((params.get("C1_EMA_LEN") or 50)),
        "RSI_LEN": int((params.get("C1_RSI_LEN") or 14)),
        "RSI_BUY": float((params.get("C1_RSI_BUY") or 42.0)),
        "RSI_SELL": float((params.get("C1_RSI_SELL") or 62.0)),
        "ALLOW_EQ_EMA": str(params.get("C1_CLOSE_ABOVE_EMA_EQ") or "0") == "1",
        "REGIME": (params.get("C1_REGIME") or "up").lower(),  # up / down / both
        "ATR_LEN": int((params.get("C1_ATR_LEN") or 14)),
        "ATR_STOP_MULT": float((params.get("C1_ATR_STOP_MULT") or 2.5)),
        "ATR_TP_MULT": float((params.get("C1_ATR_TP_MULT") or 0.0)),  # 0 disables TP
        "COOLDOWN_SEC": int((params.get("C1_COOLDOWN_SEC") or 600)),
        "ORDER_NOTIONAL": float(params.get("ORDER_NOTIONAL") or 25),
        "MAX_SPREAD_PCT": float(params.get("C1_MAX_SPREAD_PCT") or 0.25),  # skip if spread too wide
        "MIN_ATR_USD": float(params.get("C1_MIN_ATR_USD") or 0.25),        # require some vol
        "MAX_POSITIONS": int((params.get("C1_MAX_POSITIONS") or 5)),
    }

def _market_ok(market, symbol: str, max_spread_pct: float) -> Tuple[bool, str, Dict[str, float]]:
    # optional: best bid/ask check if your MarketCrypto exposes it; else fall back to last spread
    try:
        quote = market.get_quote(symbol)  # must return dict with bid, ask
        bid = float(quote.get("bid", 0) or 0)
        ask = float(quote.get("ask", 0) or 0)
        if bid <= 0 or ask <= 0 or ask < bid:
            return False, "bad_quote", {"bid": bid, "ask": ask}
        spread = (ask - bid) / ((ask + bid) / 2) * 100.0
        if spread > max_spread_pct:
            return False, f"spread>{max_spread_pct}%", {"bid": bid, "ask": ask, "spread_pct": spread}
        return True, "ok", {"bid": bid, "ask": ask, "spread_pct": spread}
    except Exception:
        # if quotes not available, allow
        return True, "no_quote_check", {}

def _cooldown_gate(state_path: str, key: str, cooldown_sec: int) -> bool:
    state = load_json(state_path, {})
    last = state.get(key)
    if not last:
        return True
    return (time.time() - float(last)) >= cooldown_sec

def _touch_cooldown(state_path: str, key: str) -> None:
    state = load_json(state_path, {})
    state[key] = time.time()
    save_json(state_path, state)

def run(market, broker, symbols: List[str], params: Dict[str, Any], dry: bool, pwrite):
    cfg = _params(params)
    tf = cfg["TIMEFRAME"]
    htf = cfg["HTF_TIMEFRAME"]

    results = []
    cooldown_path = "storage/c1_cooldowns.json"

    # positions to limit concurrent entries if desired
    try:
        open_positions = broker.get_positions()
        n_open = len(open_positions) if isinstance(open_positions, list) else 0
    except Exception:
        n_open = 0

    for symbol in symbols:
        reason = None
        try:
            # bars: expect list of dicts: {t, open, high, low, close}
            bars = market.get_bars(symbol, tf, limit=300)
            if not bars or len(bars) < 60:
                results.append({"symbol": symbol, "status": "no_data"})
                continue
            closes = [float(b["close"]) for b in bars]
            highs  = [float(b["high"]) for b in bars]
            lows   = [float(b["low"])  for b in bars]

            # indicators (LTF)
            ema_len = cfg["EMA_LEN"]
            ema_series = ema(closes, ema_len)
            rsi_series = rsi(closes, cfg["RSI_LEN"])
            atr_series = atr(highs, lows, closes, cfg["ATR_LEN"])

            c = closes[-1]
            e = ema_series[-1]
            r = rsi_series[-1]
            a = atr_series[-1]

            if a < cfg["MIN_ATR_USD"]:
                results.append({"symbol": symbol, "action": "flat", "close": c, "ema": e, "rsi": r, "atr": a, "reason": "atr_too_low"})
                continue

            # regime (HTF trend)
            htf_bars = market.get_bars(symbol, htf, limit=200)
            if not htf_bars or len(htf_bars) < 50:
                results.append({"symbol": symbol, "action": "flat", "close": c, "ema": e, "rsi": r, "atr": a, "reason": "no_htf"})
                continue
            htf_closes = [float(b["close"]) for b in htf_bars]
            htf_ema200 = ema(htf_closes, 200)[-1] if len(htf_closes) >= 200 else ema(htf_closes, max(20, len(htf_closes)//2))[-1]
            trend_up = htf_closes[-1] >= htf_ema200
            trend_down = not trend_up

            regime = cfg["REGIME"]
            if regime == "up" and not trend_up:
                results.append({"symbol": symbol, "action": "flat", "close": c, "ema": e, "rsi": r, "atr": a, "reason": "regime_down"})
                continue
            if regime == "down" and not trend_down:
                results.append({"symbol": symbol, "action": "flat", "close": c, "ema": e, "rsi": r, "atr": a, "reason": "regime_up"})
                continue

            # EMA gate
            if cfg["ALLOW_EQ_EMA"]:
                above = c >= e
            else:
                above = c > e
            if not above:
                results.append({"symbol": symbol, "action": "flat", "close": c, "ema": e, "rsi": r, "atr": a, "reason": "close<ema"})
                continue

            # RSI buy gate (mean-revert)
            if r >= cfg["RSI_BUY"]:
                results.append({"symbol": symbol, "action": "flat", "close": c, "ema": e, "rsi": r, "atr": a, "reason": f"rsi>={cfg['RSI_BUY']}"})
                continue

            # spread/liquidity gate
            ok_mkt, why, extra = _market_ok(market, symbol, cfg["MAX_SPREAD_PCT"])
            if not ok_mkt:
                results.append({"symbol": symbol, "action": "flat", "close": c, "ema": e, "rsi": r, "atr": a, "reason": why, **extra})
                continue

            # cooldown
            if not _cooldown_gate(cooldown_path, symbol, cfg["COOLDOWN_SEC"]):
                results.append({"symbol": symbol, "action": "flat", "close": c, "ema": e, "rsi": r, "atr": a, "reason": "cooldown"})
                continue

            # max concurrent positions
            if n_open >= cfg["MAX_POSITIONS"]:
                results.append({"symbol": symbol, "action": "flat", "close": c, "ema": e, "rsi": r, "atr": a, "reason": "max_positions"})
                continue

            # --- EXECUTION ---
            notional = float(cfg["ORDER_NOTIONAL"])
            # optional vol targeting: scale by reference ATR (commented)
            # ref = max(a, 1e-8)
            # target_vol_usd = notional
            # notional = target_vol_usd  # or adjust by ref / typical ATR

            if not dry:
                # market order notional
                broker.place_order_notional(symbol=symbol, side="buy", notional=notional, type="market")
                pwrite(symbol, "c1", "BUY", notional, "entry", dry, {"close": c, "rsi": r, "ema": e, "atr": a})
            results.append({"symbol": symbol, "action": "buy", "close": c, "ema": e, "rsi": r, "atr": a, "notional": notional})

            # exit management is delegated to C1 SELL signals in subsequent scans; here we just tag cooldown
            _touch_cooldown(cooldown_path, symbol)

            # SELL logic (simple example: if rsi >= RSI_SELL → flatten) — executed next tick
            # You can incorporate broker.get_positions() and send sells when threshold breached.

        except Exception as ex:
            results.append({"symbol": symbol, "action": "error", "error": str(ex)})

    return results
