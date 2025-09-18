# strategies/c1.py
from __future__ import annotations
import math
from typing import Any, Dict, Iterable, List, Callable, Optional
import pandas as pd

__version__ = "1.1.2"
VERSION = (1, 1, 2)

# --------------------------
# Small helpers (no imports)
# --------------------------
def _nz(x, alt=0.0):
    try:
        if x is None or (isinstance(x, float) and math.isnan(x)):
            return alt
        return x
    except Exception:
        return alt

def _last(s: pd.Series, default=float("nan")) -> float:
    try:
        if s is None or len(s) == 0:
            return default
        return float(s.iloc[-1])
    except Exception:
        return default

def _len_ok(df: pd.DataFrame, n: int) -> bool:
    try:
        return isinstance(df, pd.DataFrame) and len(df) >= n
    except Exception:
        return False

def _ensure_cols(df: pd.DataFrame, cols: Iterable[str]) -> bool:
    try:
        return all(c in df.columns for c in cols)
    except Exception:
        return False

def _safe_float(v, default: float) -> float:
    try:
        return float(v)
    except Exception:
        return default

def ema(series: pd.Series, length: int) -> pd.Series:
    return series.astype(float).ewm(span=int(length), adjust=False).mean()

def rsi(series: pd.Series, length: int = 14) -> pd.Series:
    series = series.astype(float)
    delta = series.diff()
    up = delta.clip(lower=0.0)
    down = -delta.clip(upper=0.0)
    roll_up = up.ewm(alpha=1/length, adjust=False).mean()
    roll_down = down.ewm(alpha=1/length, adjust=False).mean().replace(0.0, 1e-12)
    rs = roll_up / roll_down
    return 100 - (100 / (1 + rs))

# --------------------------
# Market/Broker duck-typing
# --------------------------
def _fetch_df(market: Any, symbol: str, cfg: Dict[str, Any]) -> Optional[pd.DataFrame]:
    """
    Try common method names to get OHLCV DataFrame with columns:
    open, high, low, close, volume (index in ascending time).
    """
    tf = cfg.get("CRYPTO_TF", "5Min")
    limit = int(cfg.get("CRYPTO_LOOKBACK", 400))

    candidates: List[Callable[..., Any]] = []
    for name in ("get_history", "history", "get_ohlcv", "fetch_ohlcv", "bars", "get_bars"):
        if hasattr(market, name):
            candidates.append(getattr(market, name))

    for fn in candidates:
        try:
            # Try a few arg shapes
            for args in (
                (symbol, tf, limit),
                (symbol, tf),
                (symbol,),
            ):
                try:
                    df = fn(*args)  # type: ignore[arg-type]
                    if isinstance(df, pd.DataFrame) and _ensure_cols(df, ["open","high","low","close"]):
                        if "volume" not in df.columns:
                            df["volume"] = 0.0
                        return df
                except TypeError:
                    continue
        except Exception:
            continue
    return None

def _place_order(broker: Any, symbol: str, side: str, notional: float) -> Any:
    """
    Try common broker method shapes; return order id or dict.
    """
    for name in ("place_order", "submit_order", "submit", "order", "create_order", "buy" if side=="buy" else "sell"):
        if hasattr(broker, name):
            fn = getattr(broker, name)
            try:
                # Try notional route first
                return fn(symbol=symbol, side=side, notional=notional)
            except TypeError:
                try:
                    # Some libs: qty in dollars (crypto notional)
                    return fn(symbol, side, notional)
                except Exception:
                    pass
            except Exception:
                pass
    # Fallback: no-op
    return {"status": "no_broker_method"}

def _pwrite_from(kwargs: Dict[str, Any]) -> Callable[[str], None]:
    pw = kwargs.get("pwrite")
    if callable(pw):
        return pw
    def _noop(_msg: str): 
        return None
    return _noop

# --------------------------
# Single-symbol core logic
# --------------------------
def _run_single(symbol: str, df: pd.DataFrame, cfg: Dict[str, Any],
                broker: Optional[Any], dry: bool) -> Dict[str, Any]:
    """
    RSI pullback above EMA (long-only).
    Knobs:
      C1_RSI_LEN (14), C1_EMA_LEN (21), C1_RSI_BUY (48),
      C1_CLOSE_ABOVE_EMA_EQ (0/1), ORDER_NOTIONAL (25), C1_MIN_BARS (>=50)
    """
    out = {"symbol": symbol, "action": "flat"}

    if not _ensure_cols(df, ["close"]):
        out.update({"reason": "missing_cols"})
        return out

    rsi_len  = int(cfg.get("C1_RSI_LEN", 14))
    ema_len  = int(cfg.get("C1_EMA_LEN", 21))
    min_bars = int(cfg.get("C1_MIN_BARS", max(50, ema_len + rsi_len + 2)))
    rsi_buy  = _safe_float(cfg.get("C1_RSI_BUY", 48), 48.0)
    allow_eq = str(cfg.get("C1_CLOSE_ABOVE_EMA_EQ", "1")) == "1"
    notional = _safe_float(cfg.get("ORDER_NOTIONAL", 25), 25.0)

    if not _len_ok(df, min_bars):
        out.update({"reason": "not_enough_bars", "have": len(df), "need": min_bars})
        return out

    close_s = df["close"].astype(float)
    ema_s   = ema(close_s, ema_len)
    rsi_s   = rsi(close_s, rsi_len)

    close = _nz(_last(close_s))
    ema_v = _nz(_last(ema_s))
    rsi_v = _nz(_last(rsi_s))

    if any(math.isnan(x) for x in [close, ema_v, rsi_v]):
        out["reason"] = "nan_indicators"
        return out

    above_ema   = (close > ema_v) or (allow_eq and abs(close - ema_v) < 1e-12)
    pullback_ok = rsi_v < rsi_buy
    enter_long  = bool(above_ema and pullback_ok)

    if enter_long and notional > 0:
        if dry or broker is None:
            out.update({
                "action": "paper_buy",
                "close": round(close, 4),
                "rsi": round(rsi_v, 2),
                "ema": round(ema_v, 2),
                "reason": "dry_run_enter_long_rsi_pullback",
            })
        else:
            oid = _place_order(broker, symbol, "buy", notional)
            out.update({
                "action": "buy",
                "order_id": oid,
                "close": round(close, 4),
                "rsi": round(rsi_v, 2),
                "ema": round(ema_v, 2),
                "reason": "enter_long_rsi_pullback",
            })
    else:
        out.update({
            "action": "flat",
            "close": round(close, 4),
            "rsi": round(rsi_v, 2),
            "ema": round(ema_v, 2),
            "reason": "no_signal",
        })
    return out

# -----------------------------------------
# Public API — accepts BOTH call styles
# -----------------------------------------
def run(*args, **kwargs):
    """
    Two valid call shapes:

    1) Inline, multi-symbol (what your app uses):
       run(market, broker, symbols, params, dry=?, pwrite=?)

    2) Single-symbol functional:
       run(symbol, df, cfg, place_order, log, dry=?)
       (place_order/log are ignored; broker/pwriter come from kwargs)
    """
    dry = bool(kwargs.get("dry", False))
    cfg = kwargs.get("params") or kwargs.get("cfg") or {}

    # Detect call shape
    if len(args) >= 4 and not isinstance(args[0], str):
        # Inline multi-symbol: (market, broker, symbols, params)
        market, broker, symbols, params = args[0], args[1], list(args[2]), dict(args[3] or {})
        cfg = {**params, **cfg}
        pwrite = _pwrite_from(kwargs)

        results: List[Dict[str, Any]] = []
        for sym in symbols:
            df = _fetch_df(market, sym, cfg)
            if df is None:
                results.append({"symbol": sym, "action": "error", "error": "no_data"})
                continue
            try:
                results.append(_run_single(sym, df, cfg, broker=None if dry else broker, dry=dry))
            except Exception as e:
                pwrite(f"[c1] error {sym}: {e}")
                results.append({"symbol": sym, "action": "error", "error": f"{type(e).__name__}: {e}"})
        return results

    else:
        # Single-symbol style: (symbol, df, cfg, place_order, log?)
        symbol = args[0] if len(args) > 0 else kwargs.get("symbol")
        df     = args[1] if len(args) > 1 else kwargs.get("df")
        cfg_in = args[2] if len(args) > 2 else kwargs.get("cfg") or {}
        cfg = {**cfg_in, **cfg}

        # Broker may be passed via kwargs in this style
        broker = kwargs.get("broker")
        if symbol is None or not isinstance(df, pd.DataFrame):
            return {"action": "error", "error": "invalid_arguments"}
        return _run_single(symbol, df, cfg, broker=None if dry else broker, dry=dry)
