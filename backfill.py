# backfill.py
import time
import math
import threading
from typing import List, Dict, Tuple, Optional

# --- hook these to your existing storage/fetch code --------------------------
# Required: you must have equivalents you use today in /diag and /scan.
# Replace the bodies with your real implementations.

def load_candles(symbol: str, timeframe: str) -> List[List[float]]:
    """Return list of [ms, o, h, l, c, v] from your cache/DB (sorted asc)."""
    # TODO: plug into your store (e.g., DB, Redis, parquet, etc.)
    return []

def save_candles(symbol: str, timeframe: str, rows: List[List[float]]) -> None:
    """Upsert/merge into your store (dedupe by timestamp, then sort)."""
    # TODO: plug into your store
    pass

def fetch_ohlcv(symbol: str, timeframe: str, since_ms: int, limit: int) -> List[List[float]]:
    """Fetch OHLCV from your data provider (exchange or vendor)."""
    # TODO: plug into your provider (this is what /diag already uses)
    return []

# --- config via env or sane defaults -----------------------------------------
import os
SYMBOLS = os.getenv("SYMBOLS", "BTC/USD,ETH/USD,SOL/USD,DOGE/USD,XRP/USD,AVAX/USD,LINK/USD,BCH/USD,LTC/USD").split(",")
NEED_5M = int(os.getenv("REQUIRED_BARS_5M", "300"))
NEED_1M = int(os.getenv("REQUIRED_BARS_1M", "1500"))
OVERSHOOT = float(os.getenv("OVERSHOOT_MULT", "2.0"))  # fetch extra to be safe
PER_CALL = int(os.getenv("FETCH_PAGE_LIMIT", "1000"))   # provider hard limit

BAR_MS = {"1Min": 60_000, "5Min": 300_000, "15Min": 900_000, "1m": 60_000, "5m": 300_000}

def bar_ms(tf: str) -> int:
    k = tf if tf in BAR_MS else tf.lower()
    if k not in BAR_MS: raise ValueError(f"Unsupported timeframe {tf}")
    return BAR_MS[k]

def need_for(tf: str) -> int:
    tfk = tf.lower()
    if tfk in ("5m","5min"): return NEED_5M
    if tfk in ("1m","1min"): return NEED_1M
    # default: 5m
    return NEED_5M

def fetch_paged(symbol: str, timeframe: str, since_ms: int, min_bars: int) -> List[List[float]]:
    out: List[List[float]] = []
    while len(out) < min_bars:
        want = min(PER_CALL, max(1, min_bars - len(out)))
        batch = fetch_ohlcv(symbol, timeframe, since_ms, want)
        if not batch: break
        out.extend(batch)
        since_ms = batch[-1][0] + 1
        time.sleep(0.05)  # gentle rate limit; adjust per provider
    return out

def merge_and_clean(existing: List[List[float]], new_rows: List[List[float]]) -> List[List[float]]:
    """Keep last write wins per timestamp, sort asc, drop nonsense/NaNs."""
    by_ts: Dict[int, List[float]] = { int(r[0]): r for r in existing }
    for r in new_rows:
        if len(r) >= 6:
            by_ts[int(r[0])] = r
    rows = [by_ts[k] for k in sorted(by_ts.keys())]
    # drop obvious bad rows (zero time/negative)
    rows = [r for r in rows if r and r[0] > 0]
    return rows

def ensure_bars(symbol: str, timeframe: str, require: Optional[int] = None) -> List[List[float]]:
    need = require or need_for(timeframe)
    have = load_candles(symbol, timeframe)
    if len(have) >= need:
        return have

    # backfill just-in-time
    lookback = int(bar_ms(timeframe) * need * OVERSHOOT)
    since = int((have[0][0] if have else int(time.time()*1000)) - lookback)
    fetched = fetch_paged(symbol, timeframe, since, min_bars=need)
    merged = merge_and_clean(have, fetched)
    save_candles(symbol, timeframe, merged)
    return merged

def resample_1m_to_5m(one_min_rows: List[List[float]]) -> List[List[float]]:
    if not one_min_rows: return []
    # Simple resampler without pandas (keeps deps small)
    out: List[List[float]] = []
    bucket_ms = bar_ms("5Min")
    cur_bucket = None
    o = h = l = c = v = None
    for ts, O, H, L, C, V in one_min_rows:
        b = (ts // bucket_ms) * bucket_ms
        if cur_bucket is None or b != cur_bucket:
            if cur_bucket is not None:
                out.append([cur_bucket, o, h, l, c, v])
            cur_bucket = b
            o = O; h = H; l = L; c = C; v = V
        else:
            h = max(h, H); l = min(l, L); c = C; v = (v or 0) + (V or 0)
    if cur_bucket is not None:
        out.append([cur_bucket, o, h, l, c, v])
    return out

def ensure_bars_5m_via_1m(symbol: str, need_5m: int) -> List[List[float]]:
    # fetch 1m bars then build 5m if native 5m is short
    one_min = ensure_bars(symbol, "1Min", require=max(NEED_1M, need_5m*5))
    five = resample_1m_to_5m(one_min)
    if len(five) >= need_5m:
        # optionally persist synthetic 5m to your cache for reuse:
        save_candles(symbol, "5Min", five)
    return five

def startup_backfill(background=True):
    def _run():
        for tf in ("1Min","5Min"):
            need = need_for(tf)
            lookback_ms = int(bar_ms(tf) * need * OVERSHOOT)
            since = int(time.time()*1000) - lookback_ms
            for sym in SYMBOLS:
                try:
                    existing = load_candles(sym, tf)
                    if len(existing) >= need:
                        continue
                    rows = fetch_paged(sym, tf, since, min_bars=need)
                    merged = merge_and_clean(existing, rows)
                    save_candles(sym, tf, merged)
                    print(f"[backfill] {sym} {tf} have={len(merged)} need={need}")
                except Exception as e:
                    print(f"[backfill] WARN {sym} {tf} {e}")
    if background:
        threading.Thread(target=_run, daemon=True).start()
    else:
        _run()
