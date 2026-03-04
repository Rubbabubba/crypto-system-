from __future__ import annotations

from . import trades_db
from .broker_kraken import trades_history

import logging
import json
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Set, List, Tuple
from uuid import uuid4

import requests
from fastapi import FastAPI, HTTPException, Request, Body
from fastapi.responses import JSONResponse
from fastapi.responses import RedirectResponse

log = logging.getLogger("crypto_light")

from .broker import balances_by_asset as _balances_by_asset
from .broker import last_balance_error as _last_balance_error
from .broker import base_asset as _base_asset
from .broker import last_price as _last_price
from .broker import market_notional as _market_notional
from .broker import get_bars as _get_bars
from .broker import best_bid_ask as _best_bid_ask
from .sizing import compute_risk_pct_equity_notional, compute_equity_fraction_notional
from .config import load_settings
from .models import WebhookPayload, WorkerExitPayload, WorkerScanPayload, WorkerExitDiagnosticsPayload, WorkerAdoptPositionsPayload
from .risk import compute_brackets, compute_atr_brackets
from .state import InMemoryState, TradePlan
from .symbol_map import normalize_symbol

settings = load_settings()


def _portfolio_exposure_usd_from_balances(balances: dict[str, float]) -> float:
    """Best-effort mark-to-market exposure for non-USD *non-stable* assets.

    We treat USD-like stables as cash elsewhere.
    We price each asset against USD using broker_kraken.last_trade_map().
    Missing prices are ignored (we prefer a scan that runs over a hard fail).
    """
    stables = {'USD', 'USDC', 'USDT'}
    exposure = 0.0
    assets = [a for a in balances.keys() if a not in stables]
    if not assets:
        return 0.0
    pairs = [f"{a}/USD" for a in assets]
    try:
        last_map = broker_kraken.last_trade_map(pairs)
    except Exception:
        return 0.0
    for a in assets:
        qty = float(balances.get(a, 0.0) or 0.0)
        if qty <= 0:
            continue
        px = last_map.get(f"{a}/USD")
        if px is None:
            continue
        try:
            exposure += qty * float(px)
        except Exception:
            continue
    return float(exposure)


def _stable_cash_usd(balances: dict[str, float]) -> float:
    """Return USD-like cash from balances.

    Kraken may report USD cash as USD or ZUSD depending on endpoint/account.
    Some accounts primarily hold stables (USDC/USDT). Treat those as cash-equivalent
    for sizing and eligibility checks.
    """
    keys = ('USD', 'ZUSD', 'USDC', 'ZUSDC', 'USDT', 'ZUSDT')
    total = 0.0
    for k in keys:
        try:
            total += float(balances.get(k, 0.0) or 0.0)
        except Exception:
            continue
    return float(total)

# Keep a normalized set for fast membership checks.
ALLOWED_SYMBOLS = set(getattr(settings, 'allowed_symbols', []) or [])

state = InMemoryState()
app = FastAPI(title="Crypto Light", version="1.0.3")


@app.middleware("http")
async def _normalize_path_slashes(request: Request, call_next):
    # Render / some clients occasionally send paths with double slashes (//trades/refresh).
    # FastAPI treats that as a different path, so we 307-redirect to the normalized path.
    path = request.scope.get('path', '')
    if '//' in path:
        while '//' in path:
            path = path.replace('//', '/')
        if path and not path.startswith('/'):
            path = '/' + path
        # Preserve query string
        qs = request.scope.get('query_string', b'')
        url = path
        if qs:
            url = url + '?' + qs.decode('utf-8', errors='ignore')
        return RedirectResponse(url=url, status_code=307)
    return await call_next(request)


def _require_worker_secret(provided: str | None) -> tuple[bool, str | None]:
    """Validate worker secret for /worker/* endpoints.

    Behavior:
    - If WORKER_SECRET is not configured (empty), allow requests.
    - If configured, require the caller to pass the same secret.
    """
    expected = (settings.worker_secret or "").strip()
    if not expected:
        return True, None

    provided = (provided or "").strip()
    if not provided:
        return False, "missing worker_secret"
    if provided != expected:
        return False, "invalid worker_secret"
    return True, None


# ---------- Entry engine (optional; replaces TradingView) ----------
ENTRY_ENGINE_ENABLED = (os.getenv("ENTRY_ENGINE_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on"))
ENTRY_ENGINE_STRATEGIES = {s.strip().lower() for s in os.getenv("ENTRY_ENGINE_STRATEGIES", "rb1,tc1").split(",") if s.strip()}
ENTRY_ENGINE_TIMEFRAME = os.getenv("ENTRY_ENGINE_TIMEFRAME", "5Min").strip() or "5Min"   # must match broker get_bars
ENTRY_ENGINE_LIMIT_BARS = int(float(os.getenv("ENTRY_ENGINE_LIMIT_BARS", "300") or 300))

# Strategy enablement (legacy strategies RB1/TC1 are always available)
STRATEGY_MODE = (os.getenv("STRATEGY_MODE", "auto") or "auto").strip().lower()  # auto|legacy
ENABLE_CR1 = (os.getenv("ENABLE_CR1", "0").strip().lower() in ("1", "true", "yes", "on"))
ENABLE_MM1 = (os.getenv("ENABLE_MM1", "0").strip().lower() in ("1", "true", "yes", "on"))

# Regime filter (benchmark is typically BTC/USD)
REGIME_FILTER_ENABLED = (os.getenv("REGIME_FILTER_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on"))
REGIME_BENCHMARK_SYMBOL = (os.getenv("REGIME_BENCHMARK_SYMBOL", "BTC/USD") or "BTC/USD").strip().upper()
REGIME_TIMEFRAME = (os.getenv("REGIME_TIMEFRAME", "60") or "60").strip()
REGIME_LIMIT_BARS = int(float(os.getenv("REGIME_LIMIT_BARS", "220") or 220))
REGIME_QUIET_MAX_24H_RANGE_PCT = float(os.getenv("REGIME_QUIET_MAX_24H_RANGE_PCT", "0.04") or 0.04)
REGIME_QUIET_ATR_LOOKBACK = int(float(os.getenv("REGIME_QUIET_ATR_LOOKBACK", "14") or 14))
REGIME_QUIET_ATR_NOW_LT_MEDIAN_MULT = float(os.getenv("REGIME_QUIET_ATR_NOW_LT_MEDIAN_MULT", "1.0") or 1.0)

# CR1 params (5m)
CR1_RANGE_LOOKBACK_BARS = int(float(os.getenv("CR1_RANGE_LOOKBACK_BARS", "288") or 288))  # ~24h of 5m
CR1_BOTTOM_PCT = float(os.getenv("CR1_BOTTOM_PCT", "0.20") or 0.20)
CR1_ATR_LEN = int(float(os.getenv("CR1_ATR_LEN", "14") or 14))
CR1_ATR_FALLING_LOOKBACK = int(float(os.getenv("CR1_ATR_FALLING_LOOKBACK", "50") or 50))
CR1_ATR_NOW_LT_MEDIAN_MULT = float(os.getenv("CR1_ATR_NOW_LT_MEDIAN_MULT", "1.0") or 1.0)
CR1_STOP_ATR_MULT = float(os.getenv("CR1_STOP_ATR_MULT", "1.2") or 1.2)
CR1_TAKE_ATR_MULT = float(os.getenv("CR1_TAKE_ATR_MULT", "1.5") or 1.5)
CR1_MAX_HOLD_SEC = int(float(os.getenv("CR1_MAX_HOLD_SEC", "2700") or 2700))
CR1_MAKER_ONLY = (os.getenv("CR1_MAKER_ONLY", "1").strip().lower() in ("1", "true", "yes", "on"))

# MM1 params
MM1_SPREAD_MIN_PCT = float(os.getenv("MM1_SPREAD_MIN_PCT", "0.0015") or 0.0015)
MM1_SPREAD_MAX_PCT = float(os.getenv("MM1_SPREAD_MAX_PCT", "0.0035") or 0.0035)
MM1_TAKE_PCT = float(os.getenv("MM1_TAKE_PCT", "0.0025") or 0.0025)
MM1_STOP_PCT = float(os.getenv("MM1_STOP_PCT", "0.006") or 0.006)
MM1_MAKER_ONLY = (os.getenv("MM1_MAKER_ONLY", "1").strip().lower() in ("1", "true", "yes", "on"))
MM1_CHASE_SEC = int(float(os.getenv("MM1_CHASE_SEC", "12") or 12))
MM1_POST_ONLY_OFFSET_PCT = float(os.getenv("MM1_POST_ONLY_OFFSET_PCT", os.getenv("POST_ONLY_OFFSET_PCT", "0.0002")) or 0.0002)
MM1_MAX_HOLD_SEC = int(float(os.getenv("MM1_MAX_HOLD_SEC", "1800") or 1800))

# RB1 params (5m)
RB1_LOOKBACK_BARS = int(float(os.getenv("RB1_LOOKBACK_BARS", "48") or 48))
RB1_BREAKOUT_BUFFER_PCT = float(os.getenv("RB1_BREAKOUT_BUFFER_PCT", "0.0005") or 0.0005)  # 0.05%

# RB1 near-breakout params (optional)
# If RB1_NEAR_BREAKOUT_PCT > 0, we will also fire when price is within this percent below breakout level
# and momentum is positive (close > prev_close) unless RB1_NEAR_REQUIRE_UP=0.
RB1_NEAR_BREAKOUT_PCT = float(os.getenv("RB1_NEAR_BREAKOUT_PCT", "0") or 0)  # e.g. 0.0015 = 0.15%
RB1_NEAR_REQUIRE_UP = int(float(os.getenv("RB1_NEAR_REQUIRE_UP", "1") or 1)) == 1
RB1_NEAR_UP_MODE = (os.getenv("RB1_NEAR_UP_MODE", "gt") or "gt").strip().lower()  # gt (default) or ge

# TC1 params
TC1_LTF_EMA = int(float(os.getenv("TC1_LTF_EMA", "20") or 20))
TC1_HTF_TIMEFRAME = os.getenv("TC1_HTF_TIMEFRAME", "60Min").strip() or "60Min"
TC1_HTF_LIMIT_BARS = int(float(os.getenv("TC1_HTF_LIMIT_BARS", "300") or 300))
TC1_HTF_FAST = int(float(os.getenv("TC1_HTF_FAST", "50") or 50))
TC1_HTF_SLOW = int(float(os.getenv("TC1_HTF_SLOW", "200") or 200))
TC1_RECLAIM_BUFFER_PCT = float(os.getenv("TC1_RECLAIM_BUFFER_PCT", "0.0005") or 0.0005)  # 0.05%

# TC1 trend-soften epsilon (optional)
# If >0, allow HTF uptrend when EMA_fast is within epsilon below EMA_slow (still requires EMA_fast rising).
TC1_TREND_SOFTEN_EPSILON = float(os.getenv("TC1_TREND_SOFTEN_EPSILON", "0") or 0)  # e.g. 0.002 = 0.2%


# ---------- Scanner config (soft allow) ----------
SCANNER_URL = os.getenv("SCANNER_URL", "").strip()
SCANNER_REFRESH_SEC = int(float(os.getenv("SCANNER_REFRESH_SEC", "300") or 300))
SCANNER_SOFT_ALLOW = (os.getenv("SCANNER_SOFT_ALLOW", "1").strip().lower() in ("1", "true", "yes", "on"))
SCANNER_TIMEOUT_SEC = float(os.getenv("SCANNER_TIMEOUT_SEC", "10") or 10)

# ---------- Universe normalization / safety constraints ----------
SCANNER_DRIVEN_UNIVERSE = (os.getenv('SCANNER_DRIVEN_UNIVERSE', '1').strip().lower() in ('1','true','yes','on'))
SCANNER_TARGET_N = int(float(os.getenv('SCANNER_TARGET_N', os.getenv('TOP_N', '5')) or 5))
UNIVERSE_USD_ONLY = (os.getenv('UNIVERSE_USD_ONLY', '0').strip().lower() in ('1','true','yes','on'))
UNIVERSE_PREFER_USD_FOR_STABLES = (os.getenv('UNIVERSE_PREFER_USD_FOR_STABLES', '0').strip().lower() in ('1','true','yes','on'))
FILTER_UNIVERSE_BY_ALLOWED_SYMBOLS = (os.getenv('FILTER_UNIVERSE_BY_ALLOWED_SYMBOLS', '0').strip().lower() in ('1','true','yes','on'))

MAX_OPEN_POSITIONS = int(float(os.getenv('MAX_OPEN_POSITIONS', '2') or 2))
MAX_ENTRIES_PER_SCAN = int(float(os.getenv('MAX_ENTRIES_PER_SCAN', '1') or 1))
MAX_ENTRIES_PER_DAY = int(float(os.getenv('MAX_ENTRIES_PER_DAY', '5') or 5))

ENABLE_CANDIDATE_RANKING = (os.getenv('ENABLE_CANDIDATE_RANKING', '1').strip().lower() in ('1','true','yes','on'))
MAX_SPREAD_PCT = float(os.getenv('MAX_SPREAD_PCT', '0.004') or 0.004)  # 0 disables
RANK_ATR_LEN = int(float(os.getenv('RANK_ATR_LEN', '14') or 14))
RANK_VOL_AVG_LEN = int(float(os.getenv('RANK_VOL_AVG_LEN', '20') or 20))

_SCANNER_CACHE: Dict[str, Any] = {
    "ts": 0.0,
    "active_symbols": set(),   # type: Set[str]
    "ok": False,
    "last_error": None,
    "raw": None,
}


def _log_event(level: str, event: Dict[str, Any]) -> None:
    try:
        state.record_event(event)
    except Exception:
        pass
    try:
        print(json.dumps(event, default=str))
    except Exception:
        print(str(event))





def get_positions() -> list[dict]:
    """Return open positions derived from Kraken balances + live USD prices.

    - Uses normalized balances from `balances_by_asset()` (e.g., "BTC", "ADA").
    - Prices are fetched per-symbol via `last_price("ASSET/USD")`.
    - Ignores *dust* positions below `settings.min_position_notional_usd` to prevent
      repeated exit/entry churn around exchange minimums.

    Return format:
      [{"symbol": "BTC/USD", "asset": "BTC", "qty": 0.01, "price": 65000, "notional_usd": 650}]
    """
    try:
        balances = _balances_by_asset() or {}
    except Exception as e:
        log.exception("balances_by_asset failed: %s", e)
        return []

    positions: list[dict] = []
    for asset, qty in balances.items():
        asset_u = str(asset).upper().strip()
        if asset_u == "USD":
            continue

        try:
            qty_f = float(qty or 0.0)
        except Exception:
            continue
        if qty_f <= 0:
            continue

        sym = f"{asset_u}/USD"
        try:
            px = float(_last_price(sym) or 0.0)
        except Exception:
            px = 0.0
        if px <= 0:
            continue

        notional = qty_f * px
        if notional < float(getattr(settings, "min_position_notional_usd", 0.0) or 0.0):
            continue

        positions.append(
            {
                "symbol": sym,
                "asset": asset_u,
                "qty": qty_f,
                "price": px,
                "notional_usd": notional,
            }
        )

    positions.sort(key=lambda x: float(x.get("notional_usd", 0.0) or 0.0), reverse=True)
    return positions

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _utc_date_str(now: Optional[datetime] = None) -> str:
    n = now or datetime.now(timezone.utc)
    return n.strftime("%Y-%m-%d")


def _is_after_utc_hhmm(now: datetime, hhmm_utc: str) -> bool:
    if not hhmm_utc:
        return False
    try:
        hh, mm = hhmm_utc.split(":", 1)
        h = int(hh)
        m = int(mm)
    except Exception:
        return False
    return (now.hour, now.minute) >= (h, m)


def _ema(values: list[float], period: int) -> list[float]:
    """Simple EMA series; returns list same length as values."""
    if period <= 1 or not values:
        return values[:]
    k = 2.0 / (period + 1.0)
    out: list[float] = []
    ema = float(values[0])
    out.append(ema)
    for v in values[1:]:
        ema = (float(v) * k) + (ema * (1.0 - k))
        out.append(ema)
    return out


def _atr_from_bars(bars: list[dict], length: int = 14) -> tuple[float | None, list[float]]:
    """Return (atr_now, atr_series) using simple moving average of True Range."""
    if not bars or len(bars) < (length + 2):
        return None, []
    trs: list[float] = []
    prev_c = float(bars[0].get("c") or 0.0)
    for b in bars[1:]:
        h = float(b.get("h") or 0.0)
        l = float(b.get("l") or 0.0)
        c = float(b.get("c") or 0.0)
        tr = max(h - l, abs(h - prev_c), abs(l - prev_c))
        trs.append(float(tr))
        prev_c = c
    if len(trs) < length:
        return None, []
    atrs: list[float] = []
    for i in range(length - 1, len(trs)):
        window = trs[i - (length - 1) : i + 1]
        atrs.append(sum(window) / float(length))
    return (float(atrs[-1]) if atrs else None), atrs


def _median(xs: list[float]) -> float | None:
    if not xs:
        return None
    s = sorted(float(x) for x in xs)
    n = len(s)
    mid = n // 2
    if n % 2 == 1:
        return float(s[mid])
    return float((s[mid - 1] + s[mid]) / 2.0)


_regime_cache: dict[str, tuple[float, dict]] = {"quiet": (0.0, {})}


def _rank_candidate(symbol: str, strategy: str, sig_debug: dict) -> dict:
    """Compute a simple quality score for candidate prioritization (higher is better)."""
    out = {"score": 0.0, "components": {}}
    try:
        bars = _get_bars(symbol, timeframe=ENTRY_ENGINE_TIMEFRAME, limit=max(120, RANK_VOL_AVG_LEN + RANK_ATR_LEN + 5))
    except Exception:
        bars = []
    if not bars or len(bars) < (RANK_VOL_AVG_LEN + 2):
        return out

    # Volume ratio (current / avg)
    vols = [float(b.get("v") or 0.0) for b in bars]
    v_now = float(vols[-1] or 0.0)
    v_avg = float(sum(vols[-RANK_VOL_AVG_LEN:]) / float(RANK_VOL_AVG_LEN)) if RANK_VOL_AVG_LEN > 0 else 0.0
    vol_ratio = (v_now / v_avg) if v_avg > 0 else 0.0

    # ATR (not directly scored heavily yet, but recorded)
    atr_now, _ = _atr_from_bars(bars, length=int(RANK_ATR_LEN or 14))

    score = 0.0
    components: dict = {
        "vol_ratio": vol_ratio,
        "atr_now": atr_now,
    }

    strat = (strategy or "").lower().strip()
    if strat == "rb1":
        meta = (sig_debug or {}).get("rb1") or {}
        dist = meta.get("dist_to_level_pct")
        near_pct = float(meta.get("near_pct") or 0.0)
        prev_close = float(meta.get("prev_close") or 0.0)
        close = float(meta.get("close") or 0.0)
        momentum = ((close - prev_close) / prev_close) if prev_close > 0 else 0.0

        # proximity: within near window gets credit; closer to level is better
        prox = 0.0
        if isinstance(dist, (int, float)) and near_pct and near_pct > 0:
            prox = max(0.0, 1.0 - (float(dist) / float(near_pct)))
        score += 2.0 * prox

        # volume expansion above avg
        score += max(0.0, vol_ratio - 1.0)

        # gentle momentum bonus
        score += max(0.0, momentum) * 0.5

        components.update({"prox": prox, "dist_to_level_pct": dist, "near_pct": near_pct, "momentum": momentum})

    elif strat == "tc1":
        meta = (sig_debug or {}).get("tc1") or {}
        close = float(meta.get("close") or 0.0)
        ltf = float(meta.get("ltf_ema") or 0.0)
        edge = ((close - ltf) / ltf) if ltf > 0 else 0.0

        score += max(0.0, edge) * 2.0
        score += max(0.0, vol_ratio - 1.0)
        components.update({"edge": edge})

    else:
        # quiet-regime strategies may have their own filters; just lightly prefer better volume
        score += max(0.0, vol_ratio - 1.0)

    out["score"] = float(score)
    out["components"] = components
    return out



def _regime_is_quiet() -> tuple[bool, dict]:
    """Best-effort regime classifier.

    Quiet means: benchmark 24h range is modest AND ATR is not expanding.
    Cached briefly to avoid hammering OHLC.
    """
    if not REGIME_FILTER_ENABLED:
        return True, {"enabled": False}

    now = time.time()
    ttl = 60.0
    cached_ts, cached_meta = _regime_cache.get("quiet", (0.0, {}))
    if (now - cached_ts) < ttl and cached_meta:
        return bool(cached_meta.get("quiet")), cached_meta

    bars = _get_bars(REGIME_BENCHMARK_SYMBOL, timeframe=REGIME_TIMEFRAME, limit=max(120, REGIME_LIMIT_BARS))
    if not bars or len(bars) < 50:
        meta = {"quiet": True, "reason": "insufficient_bars", "enabled": True}
        _regime_cache["quiet"] = (now, meta)
        return True, meta

    closes = [float(b.get("c") or 0.0) for b in bars]
    highs = [float(b.get("h") or 0.0) for b in bars]
    lows = [float(b.get("l") or 0.0) for b in bars]
    last = float(closes[-1]) if closes else 0.0

    n24 = 24 if REGIME_TIMEFRAME in ("60", "1H", "1h", "1HR") else min(len(bars), 288)
    recent_high = max(highs[-n24:])
    recent_low = min(lows[-n24:])
    range_pct = (recent_high - recent_low) / last if last > 0 else 0.0

    atr_now, atr_series = _atr_from_bars(bars, length=int(REGIME_QUIET_ATR_LOOKBACK))
    med = _median(atr_series[-60:]) if atr_series else None
    atr_ok = True
    if atr_now is not None and med is not None and med > 0:
        atr_ok = float(atr_now) <= float(med) * float(REGIME_QUIET_ATR_NOW_LT_MEDIAN_MULT)

    quiet = (float(range_pct) <= float(REGIME_QUIET_MAX_24H_RANGE_PCT)) and bool(atr_ok)
    meta = {
        "enabled": True,
        "quiet": bool(quiet),
        "benchmark": REGIME_BENCHMARK_SYMBOL,
        "timeframe": REGIME_TIMEFRAME,
        "range_24h_pct": float(range_pct),
        "atr_now": float(atr_now) if atr_now is not None else None,
        "atr_median": float(med) if med is not None else None,
        "atr_ok": bool(atr_ok),
        "range_ok": bool(float(range_pct) <= float(REGIME_QUIET_MAX_24H_RANGE_PCT)),
    }
    _regime_cache["quiet"] = (now, meta)
    return bool(quiet), meta


def _signal_cr1(symbol: str) -> tuple[bool, dict]:
    bars = _get_bars(symbol, timeframe=ENTRY_ENGINE_TIMEFRAME, limit=max(ENTRY_ENGINE_LIMIT_BARS, CR1_RANGE_LOOKBACK_BARS + CR1_ATR_LEN + 10))
    if not bars or len(bars) < (CR1_ATR_LEN + 10):
        return False, {"reason": "insufficient_bars", "bars": len(bars) if bars else 0}

    closes = [float(b.get("c") or 0.0) for b in bars]
    highs = [float(b.get("h") or 0.0) for b in bars]
    lows = [float(b.get("l") or 0.0) for b in bars]
    cur_close = float(closes[-1])
    prev_close = float(closes[-2])

    look = min(len(bars) - 1, int(CR1_RANGE_LOOKBACK_BARS))
    recent_high = max(highs[-look:])
    recent_low = min(lows[-look:])
    rng = max(1e-12, recent_high - recent_low)
    pos = (cur_close - recent_low) / rng

    atr_now, atr_series = _atr_from_bars(bars, length=int(CR1_ATR_LEN))
    med = _median(atr_series[-int(CR1_ATR_FALLING_LOOKBACK):]) if atr_series else None
    atr_ok = True
    if atr_now is not None and med is not None and med > 0:
        atr_ok = float(atr_now) <= float(med) * float(CR1_ATR_NOW_LT_MEDIAN_MULT)

    bottom_ok = float(pos) <= float(CR1_BOTTOM_PCT)
    bounce_ok = cur_close > prev_close

    ok = bool(bottom_ok and atr_ok and bounce_ok)
    dbg = {
        "range_high": float(recent_high),
        "range_low": float(recent_low),
        "pos_in_range": float(pos),
        "bottom_pct": float(CR1_BOTTOM_PCT),
        "atr_now": float(atr_now) if atr_now is not None else None,
        "atr_median": float(med) if med is not None else None,
        "atr_ok": bool(atr_ok),
        "prev_close": float(prev_close),
        "close": float(cur_close),
        "bounce_ok": bool(bounce_ok),
        "bar_ts": int(float(bars[-1].get("t") or 0)),
    }
    return ok, dbg


def _signal_mm1(symbol: str) -> tuple[bool, dict]:
    """Maker-capture trigger (only valid in quiet regime)."""
    try:
        from . import broker_kraken as _bk
        pair = _bk.to_kraken(symbol)
        bid, ask = _bk._best_bid_ask(pair)  # type: ignore[attr-defined]
    except Exception:
        bid, ask = None, None

    if not bid or not ask or float(bid) <= 0 or float(ask) <= 0:
        return False, {"reason": "no_bid_ask"}

    mid = (float(bid) + float(ask)) / 2.0
    spr = (float(ask) - float(bid)) / mid if mid > 0 else 0.0
    # Use settings (env-driven) rather than module constants so deploy-time env changes
    # are always reflected in decisions + diagnostics.
    min_pct = float(getattr(settings, "mm1_spread_min_pct", MM1_SPREAD_MIN_PCT) or MM1_SPREAD_MIN_PCT)
    max_pct = float(getattr(settings, "mm1_spread_max_pct", MM1_SPREAD_MAX_PCT) or MM1_SPREAD_MAX_PCT)
    spr_ok = (float(spr) >= float(min_pct)) and (float(spr) <= float(max_pct))
    dbg = {
        "bid": float(bid),
        "ask": float(ask),
        "mid": float(mid),
        "spread_pct": float(spr),
        "spr_ok": bool(spr_ok),
        "min_spread_pct": float(min_pct),
        "max_spread_pct": float(max_pct),
    }
    return bool(spr_ok), dbg


def _strategy_max_hold_sec(strategy: str) -> int:
    s = (strategy or "").strip().lower()
    if s == "cr1":
        return int(CR1_MAX_HOLD_SEC)
    if s == "mm1":
        return int(MM1_MAX_HOLD_SEC)
    if s == "rb1":
        return int(getattr(settings, "rb1_max_hold_sec", 0) or 0)
    if s == "tc1":
        return int(getattr(settings, "tc1_max_hold_sec", 0) or 0)
    return 0


def _rb1_long_signal(symbol: str) -> tuple[bool, dict]:
    """True breakout: close crosses above prior N-bar high (edge-trigger)."""
    bars = _get_bars(symbol, timeframe=ENTRY_ENGINE_TIMEFRAME, limit=max(ENTRY_ENGINE_LIMIT_BARS, RB1_LOOKBACK_BARS + 5))
    if not bars or len(bars) < (RB1_LOOKBACK_BARS + 3):
        return False, {"reason": "insufficient_bars", "bars": len(bars) if bars else 0}

    highs = [float(b["h"]) for b in bars]
    closes = [float(b["c"]) for b in bars]
    ts = [int(float(b["t"])) for b in bars]

    # prior range high excludes current bar
    look = highs[-(RB1_LOOKBACK_BARS+1):-1]
    range_high = max(look)
    prev_close = closes[-2]
    cur_close = closes[-1]

    level = range_high * (1.0 + RB1_BREAKOUT_BUFFER_PCT)

    # Primary breakout (edge-trigger)
    breakout = (prev_close <= level) and (cur_close > level)

    # Optional near-breakout: allow early entry when close is within X% of the level.
    near = False
    near_threshold = None
    dist_pct = None
    if RB1_NEAR_BREAKOUT_PCT and RB1_NEAR_BREAKOUT_PCT > 0:
        near_threshold = level * (1.0 - RB1_NEAR_BREAKOUT_PCT)
        dist_pct = (level - cur_close) / level if level else None
        momentum_ok = ((cur_close >= prev_close) if RB1_NEAR_UP_MODE == 'ge' else (cur_close > prev_close)) if RB1_NEAR_REQUIRE_UP else True
        # Still require we haven't already broken out in prior bar to avoid repeated triggers.
        near = (prev_close < level) and (cur_close >= near_threshold) and momentum_ok

    fired = breakout or near

    meta = {
        "range_high": range_high,
        "level": level,
        "prev_close": prev_close,
        "close": cur_close,
        "bar_ts": ts[-1],
        "breakout": breakout,
        "near": near,
        "near_pct": RB1_NEAR_BREAKOUT_PCT,
        "near_threshold": near_threshold,
        "dist_to_level_pct": dist_pct,
        "require_up": RB1_NEAR_REQUIRE_UP,
        "up_mode": RB1_NEAR_UP_MODE,
    }
    return fired, meta


def _tc1_long_signal(symbol: str) -> tuple[bool, dict]:
    """Trend pullback continuation:
    - HTF uptrend: EMA_fast > EMA_slow AND EMA_fast rising
    - LTF reclaim: close crosses back above LTF EMA after being below
    """
    # HTF trend
    htf = _get_bars(symbol, timeframe=TC1_HTF_TIMEFRAME, limit=max(TC1_HTF_LIMIT_BARS, TC1_HTF_SLOW + 5))
    if not htf or len(htf) < (TC1_HTF_SLOW + 3):
        return False, {"reason": "insufficient_htf_bars", "bars": len(htf) if htf else 0}

    htf_closes = [float(b["c"]) for b in htf]
    ema_fast = _ema(htf_closes, TC1_HTF_FAST)
    ema_slow = _ema(htf_closes, TC1_HTF_SLOW)
    raw_uptrend = (ema_fast[-1] > ema_slow[-1]) and (ema_fast[-1] > ema_fast[-2])
    eps = TC1_TREND_SOFTEN_EPSILON if TC1_TREND_SOFTEN_EPSILON and TC1_TREND_SOFTEN_EPSILON > 0 else 0.0
    softened = (ema_fast[-1] >= (ema_slow[-1] * (1.0 - eps))) and (ema_fast[-1] > ema_fast[-2])
    uptrend = softened

    # LTF reclaim
    ltf = _get_bars(symbol, timeframe=ENTRY_ENGINE_TIMEFRAME, limit=max(ENTRY_ENGINE_LIMIT_BARS, TC1_LTF_EMA + 10))
    if not ltf or len(ltf) < (TC1_LTF_EMA + 5):
        return False, {"reason": "insufficient_ltf_bars", "bars": len(ltf) if ltf else 0, "uptrend": uptrend}

    ltf_closes = [float(b["c"]) for b in ltf]
    ltf_ts = [int(float(b["t"])) for b in ltf]
    ltf_ema = _ema(ltf_closes, TC1_LTF_EMA)

    prev_close = ltf_closes[-2]
    cur_close = ltf_closes[-1]
    prev_ema = ltf_ema[-2]
    cur_ema = ltf_ema[-1]

    buffer = cur_ema * TC1_RECLAIM_BUFFER_PCT
    fired = uptrend and (prev_close < (prev_ema - buffer)) and (cur_close > (cur_ema + buffer))

    meta = {
        "uptrend": uptrend,
        "uptrend_raw": raw_uptrend,
        "trend_soften_epsilon": eps,
        "htf_fast": ema_fast[-1],
        "htf_fast_prev": ema_fast[-2],
        "htf_slow": ema_slow[-1],
        "ltf_ema": cur_ema,
        "prev_close": prev_close,
        "close": cur_close,
        "bar_ts": ltf_ts[-1],
    }
    return fired, meta



def _within_minutes_after_utc_hhmm(now: datetime, hhmm_utc: str, window_min: int) -> bool:
    if not hhmm_utc:
        return False
    try:
        hh, mm = hhmm_utc.split(":", 1)
        h = int(hh)
        m = int(mm)
    except Exception:
        return False
    start = h * 60 + m
    cur = now.hour * 60 + now.minute
    return start <= cur < (start + max(1, int(window_min)))


def _is_flatten_time(now: datetime, hhmm_utc: str) -> bool:
    try:
        hh, mm = hhmm_utc.split(":", 1)
        h = int(hh)
        m = int(mm)
    except Exception:
        h, m = 23, 55
    return (now.hour, now.minute) >= (h, m)


def _validate_symbol(symbol: str) -> str:
    sym = normalize_symbol(symbol)
    if sym not in settings.allowed_symbols:
        raise HTTPException(status_code=400, detail=f"symbol_not_allowed: {sym}")
    return sym


def _has_position(symbol: str) -> tuple[bool, float]:
    """Return (has_position, position_notional_usd).

    IMPORTANT: treat tiny 'dust' balances as *not* a position so they don't block entries/exits.
    """
    bal = _balances_by_asset()
    base = _base_asset(symbol)
    qty = float(bal.get(base, 0.0) or 0.0)
    if qty <= 0:
        return False, 0.0

    px = float(_last_price(symbol) or 0.0)
    notional = float(qty * px) if px > 0 else 0.0

    # IMPORTANT:
    # Use the module-level `settings` (loaded from env at startup). A previous refactor
    # attempted to call `_get_settings()` here, but that function does not exist, which
    # silently disabled the dust filter (caught by the broad exception handler).
    try:
        min_pos = float(getattr(settings, "min_position_notional_usd", 0.0) or 0.0)
        if min_pos and notional < min_pos:
            return False, notional
    except Exception:
        # If anything goes wrong, fail *open* (treat as position) to avoid accidental
        # double-entry. Callers will still have other safety gates.
        return True, notional

    return True, notional


# ---------- Scanner helpers ----------
def _scanner_should_refresh() -> bool:
    if not SCANNER_URL:
        return False
    return (time.time() - float(_SCANNER_CACHE["ts"] or 0.0)) >= float(SCANNER_REFRESH_SEC)


def _scanner_fetch_active_symbols() -> list[str]:
    """Fetch scanner / knows how to normalize symbols and apply dedup/filters.
    Returns a list of *normalized* symbols (e.g. BTC/USD) suitable for allow-checks.
    Never raises; returns [] on failure.
    """
    try:
        url = settings.scanner_url
        if not url:
            return []
        r = requests.get(url, timeout=settings.scanner_timeout_sec)
        r.raise_for_status()
        data = r.json()
        syms = data.get("active_symbols") or []
        out: list[str] = []
        for s in syms:
            try:
                out.append(normalize_symbol(str(s)))
            except Exception:
                continue
        # de-dupe preserve order
        seen=set()
        dedup=[]
        for s in out:
            if s in seen:
                continue
            seen.add(s)
            dedup.append(s)
        return dedup
    except Exception:
        return []



def _scanner_fetch_active_symbols_and_meta() -> tuple[bool, str | None, dict, list[str]]:
    """Fetch active symbols + include scanner meta/diagnostics.

    Returns: (ok, reason, meta, symbols)
    - ok: True if HTTP 200 and JSON parsed
    - reason: short string when ok=False
    - meta: dict of useful fields (status_code, url, last_error, last_refresh_utc, etc)
    - symbols: list[str] of normalized symbols (e.g., 'ETH/USD')

    Never raises.
    """
    url = (SCANNER_URL or os.getenv("SCANNER_URL", "").strip())
    meta: dict = {"scanner_url": url}
    if not url:
        return False, "missing_scanner_url", meta, []

    try:
        r = requests.get(url, timeout=SCANNER_TIMEOUT_SEC)
        meta["status_code"] = r.status_code
        try:
            meta["elapsed_ms"] = int(getattr(getattr(r, "elapsed", None), "total_seconds", lambda: 0)() * 1000)
        except Exception:
            meta["elapsed_ms"] = None

        if r.status_code != 200:
            meta["body_snippet"] = (r.text or "")[:500]
            return False, f"http_{r.status_code}", meta, []

        data = r.json() if r.content else {}
        meta["scanner_ok"] = bool(data.get("ok", True))
        meta["last_error"] = data.get("last_error")
        meta["last_refresh_utc"] = data.get("last_refresh_utc")
        if isinstance(data.get("meta"), dict):
            meta["scanner_meta"] = data.get("meta")

        raw_syms = data.get("active_symbols")
        if not isinstance(raw_syms, list):
            return False, "invalid_active_symbols", meta, []

        clean: list[str] = []
        for s in raw_syms:
            if not isinstance(s, str):
                continue
            try:
                clean.append(normalize_symbol(s))
            except Exception:
                continue

        # de-dupe preserve order
        seen: set[str] = set()
        dedup: list[str] = []
        for s in clean:
            if s in seen:
                continue
            seen.add(s)
            dedup.append(s)

        return True, None, meta, dedup

    except Exception as e:
        meta["error"] = f"{type(e).__name__}: {e}"
        return False, "exception", meta, []
def _build_universe(payload, scanner_syms: list[str]) -> list[str]:
    """Build the scan universe safely.

    Priority:
      1) Explicit payload.symbols (if provided and non-empty)
      2) Scanner symbols (scanner_syms)  <-- primary mode
      3) Fallback to settings.allowed_symbols

    Optional behaviors (env-driven):
      - SCANNER_TARGET_N: cap universe size from scanner (default 5)
      - UNIVERSE_USD_ONLY: keep only */USD
      - UNIVERSE_PREFER_USD_FOR_STABLES: when USD-only, convert BASE/USDT or BASE/USDC -> BASE/USD
        *only if BASE/USD exists in scanner list*, else drop it.
      - FILTER_UNIVERSE_BY_ALLOWED_SYMBOLS: intersect universe with ALLOWED_SYMBOLS (default off)
    """
    try:
        requested = getattr(payload, "symbols", None) or []
        force_scan = bool(getattr(payload, "force_scan", False))
    except Exception:
        requested, force_scan = [], False

    def _norm_list(items):
        out: list[str] = []
        seen: set[str] = set()
        for s in items or []:
            try:
                sym = normalize_symbol(str(s))
            except Exception:
                continue
            if sym in seen:
                continue
            seen.add(sym)
            out.append(sym)
        return out

    requested_norm = _norm_list(requested)
    scanner_norm = _norm_list(scanner_syms)

    # Build raw universe
    if requested_norm:
        universe = requested_norm
    elif scanner_norm:
        universe = scanner_norm[: max(1, int(SCANNER_TARGET_N or 5))] if SCANNER_DRIVEN_UNIVERSE else scanner_norm
    else:
        universe = _norm_list(getattr(settings, "allowed_symbols", []) or [])

    # USD-only filtering / conversion
    if UNIVERSE_USD_ONLY:
        scanner_set = set(scanner_norm)
        out: list[str] = []
        for s in universe:
            try:
                base, quote = s.split("/", 1)
            except Exception:
                continue
            if quote == "USD":
                out.append(s)
                continue
            if UNIVERSE_PREFER_USD_FOR_STABLES and quote in ("USDT", "USDC"):
                cand = f"{base}/USD"
                if cand in scanner_set:
                    out.append(cand)
                # else: drop (don't silently trade a different quote)
        # de-dupe preserve order
        universe = list(dict.fromkeys(out))

    # Allowed-symbol filtering (off by default for scanner-driven universe)
    allowed = set(getattr(settings, "allowed_symbols", []) or [])
    if allowed and FILTER_UNIVERSE_BY_ALLOWED_SYMBOLS and not (force_scan and SCANNER_SOFT_ALLOW):
        universe = [s for s in universe if s in allowed]

    # Stable fallback: if universe empty but allowed configured, use allowed
    if not universe and allowed:
        universe = list(dict.fromkeys(list(allowed)))

    return universe


def _scanner_refresh() -> None:
    if not SCANNER_URL:
        return

    try:
        r = requests.get(SCANNER_URL, timeout=SCANNER_TIMEOUT_SEC)
        r.raise_for_status()
        j = r.json()

        # Expected: {"ok": true, "active_symbols": ["SOL/USD", ...], ...}
        active = j.get("active_symbols") or j.get("active_coins") or []
        active_norm = set()

        for s in active:
            try:
                active_norm.add(normalize_symbol(str(s)))
            except Exception:
                continue

        _SCANNER_CACHE["ts"] = time.time()
        _SCANNER_CACHE["active_symbols"] = active_norm
        _SCANNER_CACHE["ok"] = bool(j.get("ok", True)) and len(active_norm) > 0
        _SCANNER_CACHE["last_error"] = None
        _SCANNER_CACHE["raw"] = j

    except Exception as e:
        _SCANNER_CACHE["ts"] = time.time()
        _SCANNER_CACHE["ok"] = False
        _SCANNER_CACHE["last_error"] = str(e)


def _symbol_allowed_by_scanner(symbol: str) -> tuple[bool, str, Dict[str, Any]]:
    """
    Returns (allowed, reason, meta)
    Soft allow:
      - If scanner is down OR returns empty => allow (fallback)
      - If scanner is healthy => allow only if symbol is in active set
    """
    if not SCANNER_URL:
        return True, "scanner_disabled", {"scanner_url": ""}

    if _scanner_should_refresh():
        _scanner_refresh()

    ok = bool(_SCANNER_CACHE.get("ok"))
    active = _SCANNER_CACHE.get("active_symbols") or set()
    err = _SCANNER_CACHE.get("last_error")

    meta = {
        "scanner_ok": ok,
        "scanner_error": err,
        "scanner_active_count": len(active),
        "scanner_soft_allow": SCANNER_SOFT_ALLOW,
    }

    if not ok or len(active) == 0:
        if SCANNER_SOFT_ALLOW:
            return True, "scanner_fallback_allow", meta
        return False, "scanner_unavailable_block", meta

    if symbol in active:
        return True, "scanner_active_allow", meta

    return False, "not_in_active_set", meta


@app.get("/health")
def health():
    scanner_ok, scanner_reason, scanner_meta, scanner_syms = _scanner_fetch_active_symbols_and_meta()
    return {
        "ok": True,
        "utc": utc_now_iso(),
        "scanner_url": SCANNER_URL or None,
        "scanner_ok": scanner_ok and (len(scanner_syms) > 0),
        "scanner_reason": scanner_reason,
        "scanner_active_count": len(scanner_syms),
        "scanner_last_refresh_utc": (scanner_meta or {}).get("last_refresh_utc"),
        "scanner_last_error": (scanner_meta or {}).get("last_error"),
        "scanner_soft_allow": SCANNER_SOFT_ALLOW,
    }



# --- Trades persistence + performance endpoints (SQLite on Render disk) ---

def _refresh_trades_from_kraken(since_hours: float = 24.0, pair: str | None = None, upsert: bool = True) -> dict:
    # Fetch recent trades from Kraken private TradesHistory.
    # Note: broker_kraken.trades_history currently returns the most recent page. That's ok for short windows
    # if you run refresh periodically.
    since_ts = time.time() - float(since_hours) * 3600.0
    raw = trades_history()  # {txid: {...}}
    items = []
    for txid, t in (raw or {}).items():
        try:
            t = dict(t)
            t["txid"] = txid
            if pair and t.get("pair") != pair:
                continue
            if float(t.get("time") or 0) < since_ts:
                continue
            items.append(t)
        except Exception:
            continue

    # Sort by time ascending for nicer inserts
    items.sort(key=lambda x: float(x.get("time") or 0))
    if upsert:
        up = trades_db.upsert_trades(items)
    else:
        up = {"ok": True, "db_path": trades_db.ensure_db(), "inserted": 0, "updated": 0, "received": len(items), "note": "upsert disabled"}
    return {"ok": True, "since_hours": float(since_hours), "pair": pair, "count": len(items), "upsert": up}

@app.get("/trades")
def get_trades(
    since_hours: float = 24.0,
    pair: str | None = None,
    limit: int = 200,
    refresh: bool = False,
    refresh_upsert: bool | None = None,
):
    if refresh:
        _refresh_trades_from_kraken(since_hours=since_hours, pair=pair, upsert=True if refresh_upsert is None else bool(refresh_upsert))
    return trades_db.query_trades(since_hours=since_hours, pair=pair, limit=limit)

@app.get("/trades/refresh")
def refresh_trades_get(
    since_hours: int = 24,
    upsert: bool = True,
    limit: int = 1000,
):
    """Back-compat convenience endpoint (GET).

    Preferred is POST /trades/refresh with JSON body.
    """
    return _refresh_trades_from_kraken(since_hours=since_hours, upsert=upsert, limit=limit)


@app.post("/trades/refresh")
def refresh_trades(body: dict | bool | None = Body(default=None)):
    # Defensive: some clients accidentally send boolean bodies ("true"/"false")
    payload = body if isinstance(body, dict) else {}
    since_hours = float(payload.get("since_hours", 24.0))
    pair = payload.get("pair")
    upsert = bool(payload.get("upsert", True))
    return _refresh_trades_from_kraken(since_hours=since_hours, pair=pair, upsert=upsert)

@app.get("/trades/summary")
def trades_summary(days: float = 7.0, refresh: bool = False, refresh_since_hours: float = 168.0):
    if refresh:
        _refresh_trades_from_kraken(since_hours=refresh_since_hours, pair=None, upsert=True)
    return trades_db.summary(days=days)


@app.get("/telemetry")
def telemetry(limit: int = 100):
    lim = max(1, min(int(limit), 500))
    return {"ok": True, "count": len(state.telemetry), "items": state.telemetry[-lim:]}



def _execute_long_entry(
    *,
    symbol: str,
    strategy: str,
    signal_name: str | None,
    signal_id: str | None,
    notional: float | None,
    source: str,
    req_id: str,
    dry_run: bool = False,
    client_ip: str | None = None,
    extra: dict | None = None,
    px_override: float | None = None,
    stop_price: float | None = None,
    take_price: float | None = None,
):
    def ignored(reason: str, **extra_fields):
        evt = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "req_id": req_id,
            "kind": source,
            "status": "ignored",
            "reason": reason,
            **(extra_fields or {}),
        }
        _log_event("warning", evt)
        return {"ok": True, "ignored": True, "reason": reason, **(extra_fields or {})}

    now = datetime.now(timezone.utc)
    utc_date = _utc_date_str(now)
    state.reset_daily_counters_if_needed(utc_date)

    if signal_id and state.seen_recent_signal(signal_id, int(settings.signal_dedupe_ttl_sec)):
        return ignored("duplicate_signal_id", symbol=symbol, strategy=strategy, signal=signal_name, signal_id=signal_id)

    if not bool(settings.trading_enabled):
        return ignored("trading_disabled", symbol=symbol, strategy=strategy, signal=signal_name, signal_id=signal_id)

    # daily loss cap
    if state.daily_pnl_usd <= -float(settings.max_daily_loss_usd):
        return ignored("max_daily_loss_reached", symbol=symbol, strategy=strategy, daily_pnl_usd=state.daily_pnl_usd)

    # cooldown
    if not state.can_enter(symbol, int(settings.entry_cooldown_sec)):
        return ignored("entry_cooldown", symbol=symbol, cooldown_sec=int(settings.entry_cooldown_sec))

    # max trades per day per symbol
    if not state.can_trade_symbol_today(symbol, int(settings.max_trades_per_symbol_per_day)):
        return ignored("max_trades_per_symbol_per_day", symbol=symbol, max_per_day=int(settings.max_trades_per_symbol_per_day))

    # one position per symbol
    has_pos, pos_notional = _has_position(symbol)
    if has_pos:
        return ignored("position_already_open", symbol=symbol, position_notional_usd=pos_notional, strategy=strategy)


    # Exposure caps (0 disables). We may need current exposure for sizing.
    max_total = float(getattr(settings, "max_total_exposure_usd", 0.0) or 0.0)
    max_symbol = float(getattr(settings, "max_symbol_exposure_usd", 0.0) or 0.0)

    positions: list[dict] = []
    total_exposure = 0.0
    sym_exposure = 0.0

    sizing_mode = str(getattr(settings, "sizing_mode", "fixed") or "fixed").strip().lower()

    if max_total or max_symbol or (notional is None and sizing_mode in ("risk_pct_equity", "equity_fraction")):
        positions = get_positions()
        total_exposure = sum(float(p.get("notional_usd", 0.0) or 0.0) for p in positions)
        sym_exposure = sum(float(p.get("notional_usd", 0.0) or 0.0) for p in positions if p.get("symbol") == symbol)

    # Determine entry notional (fixed by default; risk sizing is opt-in).
    sizing_meta: dict | None = None
    if notional is not None:
        _notional = float(notional)
    elif sizing_mode == "equity_fraction":
        bals = _balances_by_asset()
        usd_cash = float(_stable_cash_usd(bals) or 0.0)
        # Total account value proxy (cash + value of open positions). For spot, this is a safe
        # and stable basis for "use X% of account value per trade".
        equity_total = float(usd_cash) + float(_portfolio_exposure_usd_from_balances(bals))
        res = compute_equity_fraction_notional(
            equity_usd=equity_total,
            fraction=float(getattr(settings, "equity_fraction_per_trade", 0.05) or 0.05),
            min_order_notional_usd=float(settings.min_order_notional_usd),
            available_cash_usd=usd_cash,
            max_total_exposure_usd=max_total,
            current_total_exposure_usd=total_exposure,
            max_symbol_exposure_usd=max_symbol,
            current_symbol_exposure_usd=sym_exposure,
            max_notional_usd=float(getattr(settings, "max_notional_usd", 0.0) or 0.0),
        )
        sizing_meta = res.to_dict()
        # Avoid passing duplicate keys into ignored(); sizing meta may include its own 'reason'.
        sizing_meta.pop("reason", None)
        if not res.ok:
            return ignored(res.reason, symbol=symbol, strategy=strategy, **(sizing_meta or {}))
        _notional = float(res.notional_usd)
    elif sizing_mode == "risk_pct_equity":
        bals = _balances_by_asset()
        usd_cash = float(_stable_cash_usd(bals) or 0.0)
        equity_total = float(usd_cash) + float(_portfolio_exposure_usd_from_balances(bals))
        res = compute_risk_pct_equity_notional(
            equity_usd=equity_total,
            risk_per_trade=float(getattr(settings, "risk_per_trade", 0.03) or 0.03),
            stop_pct=float(settings.stop_pct),
            min_order_notional_usd=float(settings.min_order_notional_usd),
            available_cash_usd=usd_cash,
            max_total_exposure_usd=max_total,
            current_total_exposure_usd=total_exposure,
            max_symbol_exposure_usd=max_symbol,
            current_symbol_exposure_usd=sym_exposure,
            max_notional_usd=float(getattr(settings, "max_notional_usd", 0.0) or 0.0),
        )
        sizing_meta = res.to_dict()
        # Avoid passing duplicate keys into ignored(); sizing meta may include its own 'reason'.
        sizing_meta.pop("reason", None)
        if not res.ok:
            return ignored(res.reason, symbol=symbol, strategy=strategy, **(sizing_meta or {}))
        _notional = float(res.notional_usd)

    else:
        _notional = float(settings.default_notional_usd)

    if _notional < float(settings.min_order_notional_usd):
        return ignored(
            "notional_below_minimum",
            symbol=symbol,
            strategy=strategy,
            notional_usd=_notional,
            min_notional_usd=float(settings.min_order_notional_usd),
            **(sizing_meta or {}),
        )

    # Enforce exposure caps (0 disables)
    if max_total or max_symbol:
        if max_total and (total_exposure + _notional) > max_total:
            log.info("Skip entry %s: total exposure cap (%.2f + %.2f > %.2f)", symbol, total_exposure, _notional, max_total)
            return {"ok": True, "skipped": True, "reason": "max_total_exposure_usd", **(sizing_meta or {})}
        if max_symbol and (sym_exposure + _notional) > max_symbol:
            log.info("Skip entry %s: symbol exposure cap (%.2f + %.2f > %.2f)", symbol, sym_exposure, _notional, max_symbol)
            return {"ok": True, "skipped": True, "reason": "max_symbol_exposure_usd", **(sizing_meta or {})}

    # Determine reference price for fills/brackets.
    # IMPORTANT: `px` is used later for sizing and bracket calc.
    px = float(px_override) if px_override is not None else float(_last_price(symbol))

    # If caller didn't supply brackets, compute them from `px`.
    if stop_price is None or take_price is None:
        s = (strategy or "").strip().lower()
        if s == "cr1":
            bars_for_atr = _get_bars(symbol, timeframe=ENTRY_ENGINE_TIMEFRAME, limit=max(ENTRY_ENGINE_LIMIT_BARS, CR1_ATR_LEN + 40))
            atr_now, _ = _atr_from_bars(bars_for_atr or [], length=int(CR1_ATR_LEN))
            if atr_now is not None and atr_now > 0:
                stop_price = float(px) - float(atr_now) * float(CR1_STOP_ATR_MULT)
                take_price = float(px) + float(atr_now) * float(CR1_TAKE_ATR_MULT)
            else:
                stop_price, take_price = compute_brackets(px, settings.stop_pct, settings.take_pct)
        elif s == "mm1":
            stop_price, take_price = compute_brackets(px, float(MM1_STOP_PCT), float(MM1_TAKE_PCT))
        else:
            # Optional ATR-based brackets for RB1/TC1 (and any non-CR1/MM1 strategy).
            if bool(getattr(settings, "atr_brackets_enabled", False)):
                try:
                    atr_len = int(getattr(settings, "atr_bracket_len", 14) or 14)
                    bars_for_atr = _get_bars(symbol, timeframe=ENTRY_ENGINE_TIMEFRAME, limit=max(ENTRY_ENGINE_LIMIT_BARS, atr_len + 40))
                    atr_now, _ = _atr_from_bars(bars_for_atr or [], length=int(atr_len))
                    if atr_now is not None and float(atr_now) > 0:
                        stop_price, take_price = compute_atr_brackets(
                            float(px),
                            float(atr_now),
                            float(getattr(settings, "atr_stop_mult", 2.0) or 2.0),
                            float(getattr(settings, "atr_take_mult", 4.0) or 4.0),
                        )
                    else:
                        stop_price, take_price = compute_brackets(px, settings.stop_pct, settings.take_pct)
                except Exception:
                    stop_price, take_price = compute_brackets(px, settings.stop_pct, settings.take_pct)
            else:
                stop_price, take_price = compute_brackets(px, settings.stop_pct, settings.take_pct)

    # Execution profile overrides (e.g., maker-only for CR1/MM1)
    exec_mode_override = None
    post_offset_override = None
    chase_sec_override = None
    chase_steps_override = None
    market_fallback_override = None
    s = (strategy or "").strip().lower()
    if s == "cr1" and CR1_MAKER_ONLY:
        exec_mode_override = "maker_first"
        market_fallback_override = False
    if s == "mm1" and MM1_MAKER_ONLY:
        exec_mode_override = "maker_first"
        market_fallback_override = False
        chase_sec_override = int(MM1_CHASE_SEC)
        chase_steps_override = 1
        post_offset_override = float(MM1_POST_ONLY_OFFSET_PCT)

    res = _market_notional(
        symbol=symbol,
        side="buy",
        notional=_notional,
        strategy=strategy,
        price=px,
        exec_mode_override=exec_mode_override,
        post_offset_override=post_offset_override,
        chase_sec_override=chase_sec_override,
        chase_steps_override=chase_steps_override,
        market_fallback_override=market_fallback_override,
    )
    evt = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "req_id": req_id,
        "kind": source,
        "status": "dry_run" if dry_run else "executed",
        "action": "buy",
        "symbol": symbol,
        "strategy": strategy,
        "signal": signal_name,
        "signal_id": signal_id,
        "price": px,
        "notional_usd": _notional,
        "stop": float(stop_price),
        "take": float(take_price),
        "order": res,
        "client_ip": client_ip,
    }
    if extra:
        evt.update(extra)
    _log_event("info", evt)

    if res.get("ok"):
        state.mark_enter(symbol)
        state.plans[symbol] = TradePlan(
            symbol=symbol,
            strategy=strategy,
            side="buy",
            entry_price=float(px),
            stop_price=float(stop_price),
            take_price=float(take_price),
            notional_usd=float(_notional),
            opened_ts=time.time(),
            max_hold_sec=_strategy_max_hold_sec(strategy),
        )
        return {"ok": True, "executed": True, "symbol": symbol, "strategy": strategy, "price": px, "stop": float(stop_price), "take": float(take_price)}

    # Do NOT create a plan or cooldown on failed orders (e.g., insufficient funds).
    return {"ok": False, "executed": False, "symbol": symbol, "strategy": strategy, "price": px, "stop": float(stop_price), "take": float(take_price), "error": res.get("error") or "order_failed"}


@app.post("/webhook")
def webhook(payload: WebhookPayload, request: Request):
    req_id = request.headers.get("x-request-id") or str(uuid4())
    client_ip = getattr(request.client, "host", None)

    def ignored(reason: str, **extra):
        evt = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "req_id": req_id,
            "kind": "webhook",
            "status": "ignored",
            "reason": reason,
            "client_ip": client_ip,
            **extra,
        }
        _log_event("warning", evt)
        return {"ok": True, "ignored": True, "reason": reason, **extra}

    # Secret
    if not settings.webhook_secret or payload.secret != settings.webhook_secret:
        _log_event(
            "warning",
            {
                "ts": datetime.now(timezone.utc).isoformat(),
                "req_id": req_id,
                "kind": "webhook",
                "status": "rejected",
                "reason": "invalid_secret",
                "client_ip": client_ip,
            },
        )
        raise HTTPException(status_code=401, detail="invalid secret")

    # Symbol allowlist (static env allowlist)
    symbol = _validate_symbol(payload.symbol)

    # Scanner gate (dynamic top5) — soft allow enabled by env
    allowed, allow_reason, allow_meta = _symbol_allowed_by_scanner(symbol)
    if not allowed:
        return ignored(
            allow_reason,
            symbol=symbol,
            strategy=str(payload.strategy),
            signal=(payload.signal or None),
            signal_id=(payload.signal_id or None),
            **allow_meta,
        )

    side = str(payload.side).lower().strip()
    strategy = str(payload.strategy).strip()
    signal_name = (payload.signal or "").strip() or None
    signal_id = (payload.signal_id or "").strip() or None

    if side != "buy":
        return ignored("long_only_mode", symbol=symbol, side=side, strategy=strategy)

    now = datetime.now(timezone.utc)
    utc_date = _utc_date_str(now)
    state.reset_daily_counters_if_needed(utc_date)

    # Idempotency / TV retries
    if signal_id and state.seen_recent_signal(signal_id, int(settings.signal_dedupe_ttl_sec)):
        return ignored(
            "duplicate_signal_id",
            symbol=symbol,
            strategy=strategy,
            signal=signal_name,
            signal_id=signal_id,
            ttl_sec=int(settings.signal_dedupe_ttl_sec),
        )

    # Master kill switch
    if not bool(settings.trading_enabled):
        return ignored("trading_disabled", symbol=symbol, strategy=strategy, signal=signal_name, signal_id=signal_id)

    # 24/7 by default; cutoff enforced only if set
    if settings.no_new_entries_after_utc and _is_after_utc_hhmm(now, settings.no_new_entries_after_utc):
        return ignored(
            "entries_disabled_after_cutoff",
            symbol=symbol,
            strategy=strategy,
            signal=signal_name,
            signal_id=signal_id,
            cutoff_utc=settings.no_new_entries_after_utc,
            utc=now.isoformat(),
        )

    # Optional short flatten window block (off by default)
    if settings.enforce_daily_flatten and settings.block_entries_after_flatten:
        if _within_minutes_after_utc_hhmm(now, settings.daily_flatten_time_utc, window_min=3):
            return ignored(
                "entries_blocked_during_flatten_window",
                symbol=symbol,
                strategy=strategy,
                signal=signal_name,
                signal_id=signal_id,
                flatten_utc=settings.daily_flatten_time_utc,
                utc=now.isoformat(),
            )

    # Entry cooldown
    if not state.can_enter(symbol, int(settings.entry_cooldown_sec)):
        return ignored(
            "entry_cooldown_active",
            symbol=symbol,
            strategy=strategy,
            signal=signal_name,
            signal_id=signal_id,
            cooldown_sec=int(settings.entry_cooldown_sec),
        )

    # Max trades/day per symbol
    trades = int(state.trades_today_by_symbol.get(symbol, 0) or 0)
    if trades >= int(settings.max_trades_per_symbol_per_day):
        return ignored(
            "max_trades_reached",
            symbol=symbol,
            strategy=strategy,
            trades_today=trades,
            max_trades=int(settings.max_trades_per_symbol_per_day),
        )

    # One position per symbol
    has_pos, pos_notional = _has_position(symbol)
    if has_pos:
        return ignored("position_already_open", symbol=symbol, strategy=strategy, position_notional_usd=pos_notional)

    # Notional
    notional = float(payload.notional_usd or settings.default_notional_usd)
    if notional < float(settings.min_order_notional_usd):
        return ignored(
            "notional_below_minimum",
            symbol=symbol,
            strategy=strategy,
            notional_usd=notional,
            min_notional_usd=float(settings.min_order_notional_usd),
        )

    # Execute BUY
    stop_price = payload.stop_price
    take_price = payload.take_price
    # Determine entry price for brackets/notional. If the alert didn't include a price,
    # fall back to the latest trade price.
    px = float(getattr(payload, "price", 0.0) or 0.0)
    if px <= 0:
        px = float(getattr(payload, "entry_price", 0.0) or 0.0)
    if px <= 0:
        px = float(_last_price(symbol))
    
    if stop_price is None or take_price is None:
        stop_price, take_price = compute_brackets(px, settings.stop_pct, settings.take_pct)

    # Exposure caps (0 disables)
    max_total = float(getattr(settings, "max_total_exposure_usd", 0.0) or 0.0)
    max_symbol = float(getattr(settings, "max_symbol_exposure_usd", 0.0) or 0.0)
    if max_total or max_symbol:
        positions = get_positions()
        total_exposure = sum(float(p.get("notional_usd", 0.0) or 0.0) for p in positions)
        sym_exposure = sum(float(p.get("notional_usd", 0.0) or 0.0) for p in positions if p.get("symbol") == symbol)
        if max_total and (total_exposure + notional) > max_total:
            log.info("Skip entry %s: total exposure cap (%.2f + %.2f > %.2f)", symbol, total_exposure, notional, max_total)
            return {"ok": True, "skipped": True, "reason": "max_total_exposure_usd"}
        if max_symbol and (sym_exposure + notional) > max_symbol:
            log.info("Skip entry %s: symbol exposure cap (%.2f + %.2f > %.2f)", symbol, sym_exposure, notional, max_symbol)
            return {"ok": True, "skipped": True, "reason": "max_symbol_exposure_usd"}
    # Exposure caps (0 disables)
    max_total = float(getattr(settings, "max_total_exposure_usd", 0.0) or 0.0)
    max_symbol = float(getattr(settings, "max_symbol_exposure_usd", 0.0) or 0.0)
    if max_total or max_symbol:
        positions = get_positions()
        total_exposure = sum(float(p.get("notional_usd", 0.0) or 0.0) for p in positions)
        sym_exposure = sum(float(p.get("notional_usd", 0.0) or 0.0) for p in positions if p.get("symbol") == symbol)
        if max_total and (total_exposure + notional) > max_total:
            log.info("Skip entry %s: total exposure cap (%.2f + %.2f > %.2f)", symbol, total_exposure, notional, max_total)
            return {"ok": True, "skipped": True, "reason": "max_total_exposure_usd"}
        if max_symbol and (sym_exposure + notional) > max_symbol:
            log.info("Skip entry %s: symbol exposure cap (%.2f + %.2f > %.2f)", symbol, sym_exposure, notional, max_symbol)
            return {"ok": True, "skipped": True, "reason": "max_symbol_exposure_usd"}
    res = _market_notional(symbol=symbol, side="buy", notional=notional, strategy=strategy, price=px)

    # Log the attempt
    _log_event(
        "info",
        {
            "ts": datetime.now(timezone.utc).isoformat(),
            "req_id": req_id,
            "kind": "webhook",
            "status": "executed" if (res and res.get("ok")) else "error",
            "action": "buy",
            "symbol": symbol,
            "strategy": strategy,
            "signal": signal_name,
            "signal_id": signal_id,
            "price": px,
            "notional_usd": notional,
            "stop": float(stop_price),
            "take": float(take_price),
            "scanner_gate": {"allowed": True, "reason": allow_reason, **allow_meta},
            "order": res,
            "client_ip": client_ip,
        },
    )

    # Guard: only create the plan (and count the trade) if the entry order actually succeeded.
    if not (res and res.get("ok")):
        return {"ok": False, "action": "buy", "symbol": symbol, "error": (res or {}).get("error") or "entry_order_failed", "order": res}

    state.mark_enter(symbol)
    state.plans[symbol] = TradePlan(
        symbol=symbol,
        side="buy",
        notional_usd=notional,
        entry_price=px,
        stop_price=stop_price,
        take_price=take_price,
        strategy=strategy,
        opened_ts=now.timestamp(),
        max_hold_sec=_strategy_max_hold_sec(strategy),
    )
    n = state.inc_trade(symbol)

    return {
        "ok": True,
        "action": "buy",
        "symbol": symbol,
        "price": px,
        "stop": stop_price,
        "take": take_price,
        "trade_count_today": n,
        "signal": signal_name,
        "signal_id": signal_id,
        "scanner_gate": {"reason": allow_reason, **allow_meta},
        "result": res,
    }


@app.post("/worker/exit")
def worker_exit(payload: WorkerExitPayload):
    # Worker secret
    if settings.worker_secret:
        if not payload.worker_secret or payload.worker_secret != settings.worker_secret:
            raise HTTPException(status_code=401, detail="invalid worker secret")

    try:
        now = datetime.now(timezone.utc)
        utc_date = _utc_date_str(now)

        did_flatten = False
        if settings.enforce_daily_flatten and _is_flatten_time(now, settings.daily_flatten_time_utc) and state.last_flatten_utc_date != utc_date:
            did_flatten = True
            state.last_flatten_utc_date = utc_date

        exits: list[dict] = []
        evaluations: list[dict] = []
        bal = _balances_by_asset()

        # --- Deterministic lifecycle cycle ---
        # Universe = balances-derived holdings + state.open_positions + any planned symbols.
        # We evaluate each symbol once per cycle and take at most one action.
        pos_syms = {p.get("symbol") for p in get_positions() if p.get("symbol")}
        plan_syms = set(getattr(state, "plans", {}).keys()) if hasattr(state, "plans") else set()
        state_open = set(getattr(state, "open_positions", []) or [])
        symbols = sorted({sym for sym in (pos_syms | plan_syms | state_open) if sym})

        # Config knobs
        dust_floor = float(getattr(settings, "min_position_notional_usd", 0.0) or 0.0)
        adopt_floor = max(
            float(getattr(settings, "min_position_notional_usd", 0.0) or 0.0),
            float(getattr(settings, "exit_min_notional_usd", 0.0) or 0.0),
        )
        max_hold_default = int(getattr(settings, "max_hold_sec", 0) or 0)
        grace = int(getattr(settings, "time_exit_grace_sec", 0) or 0)
        cooldown = int(getattr(settings, "exit_cooldown_sec", 0) or 0)
        dry_run = bool(getattr(settings, "exit_dry_run", False))
        diagnostics = bool(getattr(settings, "exit_diagnostics", False))

        def _ensure_plan(symbol: str, *, qty: float, px: float) -> TradePlan | None:
            """Return an existing plan, or adopt a holding into a plan if eligible.

            Adoption is intentionally conservative:
            - Never create plans for dust holdings.
            - Never create plans below adopt_floor.
            - Adopted plans start their clock at adoption time to avoid breaking live legacy balances.
            """
            plan0 = state.plans.get(symbol)
            if plan0:
                return plan0
            pos_notional = float(qty) * float(px)
            if dust_floor and pos_notional < dust_floor:
                # Dust holdings: do not track or exit.
                state.plans.pop(symbol, None)
                state.clear_pending_exit(symbol)
                return None
            if pos_notional < adopt_floor:
                return None

            entry_px = float(px) if px > 0 else float(_last_price(symbol) or 0.0)
            if entry_px <= 0:
                return None
            stop_px, take_px = compute_brackets(entry_px, settings.stop_pct, settings.take_pct)
            plan_new = TradePlan(
                symbol=symbol,
                side="buy",
                notional_usd=float(settings.default_notional_usd),
                entry_price=float(entry_px),
                stop_price=float(stop_px),
                take_price=float(take_px),
                strategy="unknown",
                opened_ts=now.timestamp(),
            )
            state.plans[symbol] = plan_new
            return plan_new

        def _maybe_apply_breakeven(plan: TradePlan, *, px: float, entry_px: float) -> None:
            """Move stop to break-even once, if enabled and trigger reached."""
            if not bool(getattr(settings, "breakeven_enabled", False)):
                return
            if bool(getattr(plan, "breakeven_armed", False)):
                return
            trigger = float(getattr(settings, "breakeven_trigger_pct", 0.0) or 0.0)
            offset = float(getattr(settings, "breakeven_offset_pct", 0.0) or 0.0)
            if trigger <= 0 or entry_px <= 0 or px <= 0:
                return
            if px < (entry_px * (1.0 + trigger)):
                return

            be_stop = entry_px * (1.0 + offset)
            # Never loosen the stop; only tighten it upward.
            if be_stop > float(getattr(plan, "stop_price", 0.0) or 0.0):
                plan.stop_price = float(be_stop)
            plan.breakeven_armed = True
            plan.breakeven_triggered_ts = now.timestamp()
            _log_event(
                "info",
                {
                    "ts": datetime.now(timezone.utc).isoformat(),
                    "kind": "plan_update",
                    "status": "breakeven_set",
                    "symbol": plan.symbol,
                    "strategy": plan.strategy,
                    "entry_price": float(entry_px),
                    "price": float(px),
                    "stop_price": float(plan.stop_price),
                    "trigger_pct": float(trigger),
                    "offset_pct": float(offset),
                },
            )

        def _choose_exit_reason(*, px: float, plan: TradePlan, age_sec: float, max_hold_sec: int) -> tuple[str | None, dict]:
            """Select a single exit action for this cycle (or None), plus a decision trace."""
            eligible = {
                "eligible_daily_flatten": bool(did_flatten),
                "eligible_stop": (px <= float(plan.stop_price)) and (age_sec >= float(getattr(settings, "min_hold_sec_before_stop", 0) or 0)),
                "eligible_take": px >= float(plan.take_price),
                "eligible_time": (max_hold_sec > 0 and age_sec >= float(max_hold_sec + grace)),
            }
            if eligible["eligible_daily_flatten"]:
                return "daily_flatten", eligible
            if eligible["eligible_stop"]:
                return "stop", eligible
            if eligible["eligible_take"]:
                return "take", eligible
            if eligible["eligible_time"]:
                return "time", eligible
            return None, eligible

        for symbol in symbols:
            # Phase 1: snapshot current holding
            try:
                px = float(_last_price(symbol) or 0.0)
            except Exception:
                px = 0.0
            if px <= 0:
                continue

            base = _base_asset(symbol)
            qty = float(bal.get(base, 0.0) or 0.0)
            if qty <= 0:
                # No holding: clear stale state.
                state.plans.pop(symbol, None)
                state.clear_pending_exit(symbol)
                continue

            # Phase 2: plan resolution / conservative adoption
            plan = _ensure_plan(symbol, qty=qty, px=px)
            if not plan:
                continue

            # Phase 3: derived metrics
            entry_px = float(getattr(plan, "entry_price", 0.0) or 0.0) or float(px)
            opened_ts = float(getattr(plan, "opened_ts", 0.0) or 0.0)
            age_sec = (now.timestamp() - opened_ts) if opened_ts > 0 else 0.0
            plan_max = int(getattr(plan, "max_hold_sec", 0) or 0)
            max_hold = int(plan_max if plan_max > 0 else max_hold_default)

            # Phase 4: lifecycle updates (state mutation only)
            be_before = bool(getattr(plan, "breakeven_armed", False))
            try:
                _maybe_apply_breakeven(plan, px=px, entry_px=entry_px)
            except Exception:
                # Lifecycle updates should never crash the exit loop.
                pass
            be_after = bool(getattr(plan, "breakeven_armed", False))

            # Phase 5: decide action (at most one)
            reason, trace = _choose_exit_reason(px=px, plan=plan, age_sec=age_sec, max_hold_sec=max_hold)
            # Optional diagnostics per symbol
            if diagnostics:
                evaluations.append({
                    "symbol": symbol,
                    "strategy": plan.strategy,
                    "price": float(px),
                    "qty": float(qty),
                    "entry_price": float(entry_px),
                    "stop_price": float(plan.stop_price),
                    "take_price": float(plan.take_price),
                    "age_sec": round(age_sec, 3),
                    "max_hold_sec": int(getattr(settings, "max_hold_sec", 0) or 0),
                    "breakeven_before": bool(be_before),
                    "breakeven_after": bool(be_after),
                    "breakeven_set": bool((not be_before) and be_after),
                    "decision": reason,
                    **trace,
                    "can_exit": bool(state.can_exit(symbol, cooldown)),
                    "dry_run": bool(dry_run),
                })
            if not reason:
                continue
            can_exit_now = state.can_exit(symbol, cooldown)
            if not can_exit_now:
                continue

            # Phase 6: execute
            notional_exit = max(float(settings.exit_min_notional_usd), float(plan.notional_usd))
            if dry_run:
                res = {"ok": True, "dry_run": True, "side": "sell", "symbol": symbol, "notional": notional_exit}
            else:
                res = _market_notional(symbol=symbol, side="sell", notional=notional_exit, strategy=plan.strategy, price=px)
                if bool(res.get("ok")):
                    state.mark_exit(symbol)
                    state.set_pending_exit(symbol, reason=reason, txid=res.get("txid"))
                else:
                    # Do not latch exit cooldowns or pending state if the order failed.
                    state.clear_pending_exit(symbol)
                # Stopout cooldown latch: only on successful orders (anti-churn)
                try:
                    if bool(res.get("ok")):
                        if reason == "stop" and hasattr(state, "mark_stopout"):
                            state.mark_stopout(symbol)
                        elif reason == "take" and hasattr(state, "clear_stopout"):
                            state.clear_stopout(symbol)
                except Exception:
                    pass

            evt = {
                "ts": datetime.now(timezone.utc).isoformat(),
                "kind": "exit",
                "status": "dry_run" if dry_run else "executed",
                "symbol": symbol,
                "strategy": plan.strategy,
                "reason": reason,
                "price": px,
                "notional_usd": notional_exit,
                "age_sec": round(age_sec, 3),
                "max_hold_sec": int(getattr(settings, "max_hold_sec", 0) or 0),
            }
            _log_event("info", evt)
            exits.append({"symbol": symbol, "reason": reason, "price": px, "age_sec": round(age_sec, 3), "dry_run": bool(dry_run), "result": res})

        resp = {"ok": True, "utc": now.isoformat(), "did_flatten": did_flatten, "exits": exits}
        if diagnostics:
            resp["evaluations"] = evaluations
        return resp

    except HTTPException:
        raise
    except Exception as e:
        _log_event(
            "error",
            {
                "ts": datetime.now(timezone.utc).isoformat(),
                "kind": "worker_exit",
                "status": "error",
                "error": str(e),
            },
        )
        return JSONResponse(status_code=200, content={"ok": False, "error": str(e)})



def _entry_signals_for_symbol(symbol: str, *, regime_quiet: bool) -> tuple[dict, dict]:
    """Return (signals, debug) for the given symbol.

    signals: {"rb1": bool, "tc1": bool, "cr1"?: bool, "mm1"?: bool}
    debug:   {"rb1": {...}, "tc1": {...}, ...}
    """
    signals: dict = {}
    debug: dict = {}

    rb1_fired, rb1_meta = _rb1_long_signal(symbol)
    signals["rb1"] = bool(rb1_fired)
    debug["rb1"] = rb1_meta

    tc1_fired, tc1_meta = _tc1_long_signal(symbol)
    signals["tc1"] = bool(tc1_fired)
    debug["tc1"] = tc1_meta

    # Optional strategies for quiet / choppy regimes
    if STRATEGY_MODE != "legacy" and regime_quiet:
        if ENABLE_CR1:
            cr1_fired, cr1_meta = _signal_cr1(symbol)
            signals["cr1"] = bool(cr1_fired)
            debug["cr1"] = cr1_meta
        if ENABLE_MM1:
            mm1_fired, mm1_meta = _signal_mm1(symbol)
            signals["mm1"] = bool(mm1_fired)
            debug["mm1"] = mm1_meta

    return signals, debug



def place_entry(symbol: str, *, strategy: str, req_id: str | None = None, client_ip: str | None = None, notional: float | None = None):
    """
    Wrapper used by /worker/scan_entries to execute an entry in live mode.

    Returns a tuple: (ok, reason, meta)
      - ok: bool
      - reason: str | None (present when ok=False or executed=False)
      - meta: dict (full structured execution result)
    """
    rid = req_id or str(uuid4())
    res = _execute_long_entry(
        symbol=symbol,
        strategy=strategy,
        signal_name=strategy,
        signal_id=None,
        notional=notional,
        source="scan_entries",
        req_id=rid,
        client_ip=client_ip,
        extra={"strategy": strategy},
    )
    # _execute_long_entry returns a dict like:
    #   {"ok": True, "executed": True|False, "reason": "..."} or {"ok": False, "error": "..."}
    ok = bool(res.get("ok", False))
    reason = res.get("reason") or res.get("error")
    # If ok but not executed, treat as not-ok for scan_entries "placed" reporting.
    if ok and not res.get("executed", False):
        return False, reason or "not_executed", res
    return ok, reason, res



def _count_open_positions(open_set: set[str]) -> int:
    """Count open positions safely.

    IMPORTANT: We should not count in-memory "plans" as open positions when we have a live
    broker snapshot, or we can deadlock trading after a restart (stale plans).
    """
    return len(open_set or set())
    utc_date = _utc_date_str(now)
    try:
        state.reset_daily_counters_if_needed(utc_date)
    except Exception:
        pass

    # Global / config safety checks
    if not getattr(settings, "trading_enabled", True):
        return False, "trading_disabled", {}

    # Daily entry cap (global)
    try:
        total_today = int(getattr(state, "trades_today_total", 0) or 0)
        if MAX_ENTRIES_PER_DAY > 0 and total_today >= int(MAX_ENTRIES_PER_DAY):
            return False, "max_entries_per_day_reached", {"entries_today": total_today, "max_entries_per_day": MAX_ENTRIES_PER_DAY}
    except Exception:
        pass

    # Time-of-day discipline
    try:
        if _is_after_utc_hhmm(now, getattr(settings, "no_new_entries_after_utc", "") or ""):
            return False, "blocked_by_time_window", {"no_new_entries_after_utc": getattr(settings, "no_new_entries_after_utc", "")}
    except Exception:
        pass

    # Per-symbol trade frequency cap
    try:
        count_for_symbol = int(getattr(state, "trades_today_by_symbol", {}).get(symbol, 0) or 0)
        if getattr(settings, "max_trades_per_symbol_per_day", 999) and count_for_symbol >= int(getattr(settings, "max_trades_per_symbol_per_day", 999)):
            return False, "max_trades_per_symbol_per_day_reached", {"trades_today": count_for_symbol, "max": getattr(settings, "max_trades_per_symbol_per_day", 999)}
    except Exception:
        pass

    # Cooldown (per-symbol)
    try:
        if not state.can_enter(symbol, getattr(settings, "entry_cooldown_sec", 0)):
            return False, "cooldown_active", {"cooldown_sec": getattr(settings, "entry_cooldown_sec", 0)}
    except Exception:
        pass

    # Cooldown (global)
    try:
        if hasattr(state, "can_enter_global") and not state.can_enter_global(getattr(settings, "global_entry_cooldown_sec", 0)):
            return False, "global_cooldown_active", {"global_cooldown_sec": getattr(settings, "global_entry_cooldown_sec", 0)}
    except Exception:
        pass

    # Stopout cooldown (per-symbol)
    try:
        if hasattr(state, "can_enter_after_stopout") and not state.can_enter_after_stopout(symbol, getattr(settings, "stopout_cooldown_sec", 0)):
            return False, "stopout_cooldown_active", {"stopout_cooldown_sec": getattr(settings, "stopout_cooldown_sec", 0)}
    except Exception:
        pass

    # Execute via existing engine
    try:
        ok, reason, meta = _execute_long_entry(symbol=symbol, strategy=strategy, signal_name=strategy)
        if ok:
            try:
                state.mark_enter(symbol)
                if hasattr(state, "mark_enter_global"):
                    state.mark_enter_global()
                state.inc_trade(symbol)
                # global counter (added in state.py patch)
                if hasattr(state, "inc_trade_total"):
                    state.inc_trade_total()
            except Exception:
                pass
        return bool(ok), str(reason), meta or {}
    except Exception as e:
        return False, f"exception:{type(e).__name__}", {"error": str(e)}







@app.post("/worker/exit_diagnostics")
def worker_exit_diagnostics(payload: WorkerExitDiagnosticsPayload):
    """Dry-run style view of what /worker/exit would attempt.

    No orders are placed. This is purely diagnostic.
    """
    _require_worker_secret(payload.worker_secret)

    balances = _balances_by_asset()
    symbols = payload.symbols or []
    selected = set([normalize_symbol(s) for s in symbols]) if symbols else None

    diagnostics: list[dict] = []
    for symbol, plan in list(state.plans.items()):
        sym = normalize_symbol(symbol)
        if selected is not None and sym not in selected:
            continue

        base = _base_asset(sym)
        qty = float(balances.get(base, 0.0) or 0.0)
        px = float(_last_price(sym) or 0.0)
        notional = qty * px if (qty > 0 and px > 0) else 0.0

        stop_px = float(getattr(plan, "stop_price", 0.0) or 0.0)
        take_px = float(getattr(plan, "take_price", 0.0) or 0.0)

        should_exit = False
        reason = None
        if qty <= 0 or px <= 0:
            reason = "no_qty_or_price"
        elif stop_px and px <= stop_px:
            should_exit = True
            reason = "stop_hit"
        elif take_px and px >= take_px:
            should_exit = True
            reason = "take_hit"
        else:
            reason = "no_exit_signal"

        exit_floor = float(getattr(settings, "exit_min_notional_usd", 0.0) or 0.0)
        target_notional = max(exit_floor, float(getattr(plan, "notional_usd", 0.0) or 0.0))

        ok_to_place = should_exit
        if should_exit:
            if notional <= 0:
                ok_to_place = False
            elif notional < target_notional * 0.98:
                # likely not enough qty to satisfy a market-notional sell request
                ok_to_place = False
                reason = (reason or "exit") + "_insufficient_qty_for_target_notional"

        diagnostics.append({
            "symbol": sym,
            "qty": qty,
            "price": px,
            "position_notional_usd": notional,
            "plan": {
                "side": getattr(plan, "side", None),
                "notional_usd": float(getattr(plan, "notional_usd", 0.0) or 0.0),
                "entry_price": float(getattr(plan, "entry_price", 0.0) or 0.0),
                "stop_price": stop_px,
                "take_price": take_px,
                "strategy": getattr(plan, "strategy", None),
                "opened_ts": getattr(plan, "opened_ts", None),
            },
            "should_exit": should_exit,
            "ok_to_place": ok_to_place,
            "reason": reason,
            "target_exit_notional_usd": target_notional,
        })

    return {
        "ok": True,
        "utc": utc_now_iso(),
        "plans_count": len(state.plans),
        "diagnostics": diagnostics,
    }


@app.post("/worker/adopt_positions")
def worker_adopt_positions(payload: WorkerAdoptPositionsPayload):
    """Create TradePlans for current Kraken balances (so exits can work).

    This does NOT place any orders — it only builds plans in memory.
    """
    _require_worker_secret(payload.worker_secret)

    balances = _balances_by_asset()

    if payload.reset_plans:
        state.plans.clear()

    include_dust = bool(payload.include_dust)
    min_notional = float(payload.min_notional_usd) if payload.min_notional_usd is not None else None

    adopted: list[dict] = []
    skipped: list[dict] = []

    # Adopt all non-USD assets with a USD price
    for asset, qty_raw in balances.items():
        asset_u = str(asset).upper().strip()
        if asset_u == "USD":
            continue
        qty = float(qty_raw or 0.0)
        if qty <= 0:
            continue

        sym = normalize_symbol(f"{asset_u}/USD")
        px = float(_last_price(sym) or 0.0)
        if px <= 0:
            skipped.append({"asset": asset_u, "symbol": sym, "reason": "no_price"})
            continue

        notional = qty * px
        floor = min_notional if min_notional is not None else float(getattr(settings, "min_position_notional_usd", 0.0) or 0.0)
        if (not include_dust) and (notional < floor):
            skipped.append({"asset": asset_u, "symbol": sym, "notional_usd": notional, "reason": "dust"})
            continue

        if sym in state.plans:
            skipped.append({"asset": asset_u, "symbol": sym, "notional_usd": notional, "reason": "plan_exists"})
            continue

        stop_px, take_px = compute_brackets(
            entry_price=px,
            stop_pct=float(settings.stop_pct),
            take_pct=float(settings.take_pct),
        )
        plan = TradePlan(
            symbol=sym,
            side="buy",
            notional_usd=float(notional),
            entry_price=float(px),
            stop_price=float(stop_px),
            take_price=float(take_px),
            strategy="adopted",
            opened_ts=time.time(),
        )
        state.plans[sym] = plan
        adopted.append({
            "symbol": sym,
            "asset": asset_u,
            "qty": qty,
            "price": px,
            "notional_usd": notional,
            "stop_price": stop_px,
            "take_price": take_px,
        })

    return {
        "ok": True,
        "utc": utc_now_iso(),
        "adopted_count": len(adopted),
        "skipped_count": len(skipped),
        "adopted": adopted,
        "skipped": skipped,
    }

@app.post("/worker/reset_plans")
def worker_reset_plans(payload: WorkerScanPayload):
    """Admin endpoint to clear stale in-memory plans.
    Uses the same worker secret gate as scan_entries/exit.
    NOTE: In-memory state is per-instance; this clears only the current instance.
    """
    # Worker secret
    if settings.worker_secret:
        if not payload.worker_secret or payload.worker_secret != settings.worker_secret:
            raise HTTPException(status_code=401, detail="invalid worker secret")

    cleared = len(state.plans)
    state.plans.clear()
    return {"ok": True, "utc": utc_now_iso(), "cleared_plans": cleared}

@app.post("/worker/scan_entries")
def scan_entries(payload: WorkerScanPayload):
    """
    Scan the current universe for entry signals (RB1/TC1), optionally executing orders.

    This endpoint ALWAYS returns a rich diagnostics payload so you can see exactly why
    you got 0 entries (no signals, already in position, symbol not allowed, etc.).
    """
    ok, reason = _require_worker_secret(payload.worker_secret)
    if not ok:
        return JSONResponse(status_code=401, content={"ok": False, "utc": utc_now_iso(), "error": reason})

    # 1) Scanner → symbols (soft allowlist) + meta
    scanner_ok, scanner_reason, scanner_meta, scanner_syms = _scanner_fetch_active_symbols_and_meta()

    # 2) Universe (explicit symbols > allowed list (+ scanner if soft allow))
    universe = _build_universe(payload, scanner_syms)

    # 3) Snapshot positions once
    positions = get_positions()
    open_set = {p["symbol"] for p in positions if p.get("qty", 0) > 0}
    open_positions_count = _count_open_positions(open_set)

    # Regime (used to optionally enable CR1/MM1 in quiet markets)
    regime_quiet, regime_meta = _regime_is_quiet()

    # 4) Evaluate signals + build diagnostics
    per_symbol: Dict[str, Any] = {}
    candidates: List[Dict[str, Any]] = []  # [{symbol,strategy,score,rank}]
    for sym in universe:
        d: Dict[str, Any] = {
            "symbol": sym,
            "in_position": sym in open_set,
            "signals": {},
            "eligible": False,
            "skip": [],
        }

        if sym in open_set:
            d["skip"].append("already_in_position")
            per_symbol[sym] = d
            continue

        if open_positions_count >= MAX_OPEN_POSITIONS:
            d["skip"].append("max_open_positions_reached")
            d["max_open_positions"] = MAX_OPEN_POSITIONS
            per_symbol[sym] = d
            continue

        try:
            fired, sig_debug = _entry_signals_for_symbol(sym, regime_quiet=bool(regime_quiet))  # (signals, debug)
        except Exception as e:
            d["skip"].append(f"signal_error:{type(e).__name__}")
            d["signal_error"] = str(e)
            per_symbol[sym] = d
            continue

        d["signals"] = fired
        d["signal_debug"] = sig_debug

        fired_strats = [k for k, v in fired.items() if v]
        if not fired_strats:
            d["skip"].append("no_signal")
            per_symbol[sym] = d
            continue

        # Strategy preference:
        # - In quiet regime, prefer MM1/CR1 over RB1/TC1 (more frequent, inventory-aware)
        # - Otherwise, prefer RB1 over TC1 when both fire
        if STRATEGY_MODE != "legacy" and bool(regime_quiet):
            if fired.get("mm1"):
                strategy = "mm1"
            elif fired.get("cr1"):
                strategy = "cr1"
            elif fired.get("rb1"):
                strategy = "rb1"
            else:
                strategy = fired_strats[0]
        else:
            strategy = "rb1" if fired.get("rb1") else fired_strats[0]
        d["eligible"] = True
        d["chosen_strategy"] = strategy
        rank = _rank_candidate(sym, strategy, sig_debug)
        d["rank"] = rank
        candidates.append({"symbol": sym, "strategy": strategy, "score": float(rank.get("score") or 0.0), "rank": rank})
        per_symbol[sym] = d

    # Apply per-scan entry cap *after* we have all candidates so we can prioritize.
    # If ENABLE_CANDIDATE_RANKING, we also sort by a simple quality score within the same
    # strategy-priority bucket (higher score = better).
    if MAX_ENTRIES_PER_SCAN > 0 and len(candidates) > MAX_ENTRIES_PER_SCAN:
        def _pri(s: str) -> int:
            s = (s or "").lower()
            if bool(regime_quiet) and STRATEGY_MODE != "legacy":
                # Quiet regime: prioritize inventory-friendly / maker-capture first.
                return {"mm1": 0, "cr1": 1, "rb1": 2, "tc1": 3}.get(s, 9)
            # Non-quiet: prioritize directional edge.
            return {"rb1": 0, "tc1": 1, "cr1": 2, "mm1": 3}.get(s, 9)

        def _uix(sym: str) -> int:
            try:
                return universe.index(sym)
            except Exception:
                return 10**9

        if ENABLE_CANDIDATE_RANKING:
            candidates_sorted = sorted(
                candidates,
                key=lambda c: (_pri(c.get("strategy")), -(float(c.get("score") or 0.0)), _uix(c.get("symbol"))),
            )
        else:
            candidates_sorted = sorted(
                candidates,
                key=lambda c: (_pri(c.get("strategy")), _uix(c.get("symbol"))),
            )

        winners = candidates_sorted[:MAX_ENTRIES_PER_SCAN]
        winner_keys = {(w.get("symbol"), w.get("strategy")) for w in winners}

        # Mark losers in diagnostics
        for c in candidates_sorted[MAX_ENTRIES_PER_SCAN:]:
            sym = c.get("symbol")
            d = per_symbol.get(sym) or {"symbol": sym, "skip": []}
            d.setdefault("skip", [])
            d["eligible"] = False
            if "max_entries_per_scan_reached" not in d["skip"]:
                d["skip"].append("max_entries_per_scan_reached")
            per_symbol[sym] = d

        candidates = winners
    else:
        # still attach a stable ordering for diagnostics
        if ENABLE_CANDIDATE_RANKING and candidates:
            candidates = sorted(
                candidates,
                key=lambda c: (-(float(c.get("score") or 0.0)), universe.index(c.get("symbol")) if c.get("symbol") in universe else 10**9),
            )
    # 5) Execute (or dry-run)
    results: List[Dict[str, Any]] = []
    for c in candidates:
        sym = c.get('symbol')
        strategy = c.get('strategy')
        score = float(c.get('score') or 0.0)
        if payload.dry_run:
            results.append({"symbol": sym, "strategy": strategy, "status": "dry_run", "score": score})
            continue

        # Spread protection (skip very wide spreads to avoid bad fills)
        bid, ask = _best_bid_ask(sym)
        spread_pct = None
        if bid and ask and bid > 0 and ask > 0:
            mid = (bid + ask) / 2.0
            spread_pct = (ask - bid) / mid if mid else None
        if MAX_SPREAD_PCT and MAX_SPREAD_PCT > 0 and spread_pct is not None and spread_pct > MAX_SPREAD_PCT:
            meta2 = {"ok": False, "ignored": True, "reason": "spread_too_wide", "symbol": sym, "strategy": strategy, "bid": bid, "ask": ask, "spread_pct": spread_pct, "max_spread_pct": MAX_SPREAD_PCT, "score": score}
            results.append({"symbol": sym, "strategy": strategy, "ok": False, "status": "skipped", "reason": "spread_too_wide", "meta": meta2})
            # Also annotate per_symbol so diagnostics show why it didn't execute.
            d = per_symbol.get(sym) or {"symbol": sym, "skip": []}
            d.setdefault("skip", [])
            if "spread_too_wide" not in d["skip"]:
                d["skip"].append("spread_too_wide")
            d["eligible"] = False
            per_symbol[sym] = d
            continue

        ok2, reason2, meta2 = place_entry(sym, strategy=strategy)
        results.append(
            {
                "symbol": sym,
                "strategy": strategy,
                "ok": ok2,
                "status": "executed" if ok2 else "skipped",
                "reason": reason2,
                "meta": (meta2 | {"score": score}) if isinstance(meta2, dict) else meta2,
            }
        )

    # --- Equity debug (helps diagnose no_equity quickly) ---
    bal_dbg = _balances_by_asset()
    stable_cash_dbg = _stable_cash_usd(bal_dbg)
    bal_keys_dbg = sorted(list(bal_dbg.keys()))[:20]
    bal_err_dbg = _last_balance_error()
    
    # 6) Return diagnostics
    return {
        "ok": True,
        "utc": utc_now_iso(),
        "universe_count": len(universe),
        "scanner": {
            "ok": scanner_ok,
            "reason": scanner_reason,
            "active_count": len(scanner_syms),
            "last_refresh_utc": (scanner_meta or {}).get("last_refresh_utc"),
            "last_error": (scanner_meta or {}).get("last_error"),
            "active_symbols": scanner_syms,
        },
        "results": results,
        "diagnostics": {
            "request": {
                "dry_run": payload.dry_run,
                "force_scan": getattr(payload, "force_scan", False),
                "symbols_provided": (getattr(payload, "symbols", None) or []),
            },
            "config": {
                "scanner_url": SCANNER_URL,
                "scanner_soft_allow": SCANNER_SOFT_ALLOW,
                "allowed_symbols_count": len(ALLOWED_SYMBOLS),
                "allowed_symbols_sample": sorted(list(ALLOWED_SYMBOLS))[:50],
                "scanner_driven_universe": SCANNER_DRIVEN_UNIVERSE,
                "scanner_target_n": SCANNER_TARGET_N,
                "universe_usd_only": UNIVERSE_USD_ONLY,
                "prefer_usd_for_stables": UNIVERSE_PREFER_USD_FOR_STABLES,
                "filter_universe_by_allowed_symbols": FILTER_UNIVERSE_BY_ALLOWED_SYMBOLS,
                "max_open_positions": MAX_OPEN_POSITIONS,
                "max_entries_per_scan": MAX_ENTRIES_PER_SCAN,
                "max_entries_per_day": MAX_ENTRIES_PER_DAY,
            },
            "regime": regime_meta,
            "equity_debug": {"stable_cash_usd": stable_cash_dbg, "balance_error": bal_err_dbg, "balance_keys": bal_keys_dbg},
            "positions_count": len(positions),
            "plans_count": len(getattr(state, "plans", {}) or {}),
            "open_positions_count": open_positions_count,
            "positions_open": sorted(list(open_set))[:50],
            "candidates": [{"symbol": c.get("symbol"), "strategy": c.get("strategy"), "score": float(c.get("score") or 0.0), "rank": (c.get("rank") or {}).get("components")} for c in candidates],
            "per_symbol": per_symbol,
        },
    }

@app.get("/dashboard")
def dashboard():
    # Dashboard is a JSON snapshot. Keep it resilient: failures here should not break the whole service.
    try:
        positions = get_positions()
    except Exception:
        positions = []

    # Safe serialization for plans/state (TradePlan is a dataclass)
    open_plans = []
    try:
        from dataclasses import asdict, is_dataclass
        for p in getattr(state, "plans", {}).values():
            open_plans.append(asdict(p) if is_dataclass(p) else p)
    except Exception:
        open_plans = []

    # Telemetry buffer is best-effort and may be absent/non-serializable
    telemetry_out = None
    try:
        telemetry_out = list(getattr(state, "telemetry", []) or [])
    except Exception:
        telemetry_out = None

    return {
        "ok": True,
        "utc": utc_now_iso(),
        "settings": {
            "max_open_positions": getattr(settings, "max_open_positions", None),
            "max_entries_per_day": getattr(settings, "max_entries_per_day", None),
            "max_entries_per_scan": getattr(settings, "max_entries_per_scan", None),
            "scanner_url": getattr(settings, "scanner_url", None),
        },
        "open_positions": positions,
        "open_plans": open_plans,
        "telemetry": telemetry_out,
    }

    t = None
    try:
        t = telemetry.snapshot()  # type: ignore[attr-defined]
    except Exception:
        try:
            t = telemetry.to_dict()  # type: ignore[attr-defined]
        except Exception:
            t = None

    return {
        "ok": True,
        "time": utc_now_iso(),
        "mode": getattr(settings, "mode", None),
        "allowed_symbols": getattr(settings, "allowed_symbols", None),
        "positions": positions,
        "open_plans": list(plans.values()) if isinstance(plans, dict) else None,
        "telemetry": t,
    }


@app.get("/performance")
def performance():
    """Very simple P&L proxy using current positions (mark-to-market).

    If you're logging fills elsewhere, you can swap this for realized P&L.
    """
    positions = get_positions()
    total_exposure = sum(float(p.get("notional_usd", 0.0) or 0.0) for p in positions)
    return {
        "ok": True,
        "time": utc_now_iso(),
        "open_positions": positions,
        "total_open_notional_usd": total_exposure,
    }