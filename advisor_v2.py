from __future__ import annotations
# advisor_v2.py
# Advisor v2 + Attribution (Phase 6 core)
#
# Source of truth: journal_v2.jsonl + telemetry_v2.jsonl + journal.db (trades table)
# All paths default under DATA_DIR (env), but can be overridden for testing.


import json
import os
import sqlite3
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple



# --- Symbol canonicalization (Step C) ---
try:
    from symbol_map import from_kraken as _from_kraken
except Exception:
    _from_kraken = None

def _canon_symbol(symbol: Any, pair: Any = None) -> str:
    """
    Return canonical UI symbol (e.g., 'ETH/USD') from potentially messy inputs:
    - 'XETHZUSD', 'XETHZ/USD', 'ETHUSD', 'ETH/USD', etc.
    """
    s = (str(symbol or "")).strip()
    p = (str(pair or "")).strip()
    # Prefer explicit symbol if present, otherwise fall back to pair
    cand = s or p
    if not cand:
        return ""
    if _from_kraken:
        try:
            out = _from_kraken(cand)
            if out:
                return out
        except Exception:
            pass
    # Basic fallback: inject slash for USD/USDT if missing
    u = cand.upper().replace(" ", "")
    if "/" not in u:
        if u.endswith("USD") and len(u) > 3:
            u = u[:-3] + "/USD"
        elif u.endswith("USDT") and len(u) > 4:
            u = u[:-4] + "/USDT"
    return u
# -----------------------
# Paths (match app.py)
# -----------------------
DATA_DIR = Path(os.getenv("DATA_DIR", "/var/data"))
JOURNAL_V2_PATH = Path(os.getenv("JOURNAL_V2_PATH", str(DATA_DIR / "journal_v2.jsonl")))
TELEMETRY_V2_PATH = Path(os.getenv("TELEMETRY_V2_PATH", str(DATA_DIR / "telemetry_v2.jsonl")))
DB_PATH = Path(os.getenv("DB_PATH", str(DATA_DIR / "journal.db")))

POLICY_CFG_DIR = Path(os.getenv("POLICY_CFG_DIR", "policy_config"))
USERREF_MAP_PATH = POLICY_CFG_DIR / "userref_map.json"

# -----------------------
# Helpers
# -----------------------

def _now_ts() -> float:
    return time.time()

def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default

def _safe_str(x: Any, default: str = "") -> str:
    try:
        return str(x)
    except Exception:
        return default

def _load_userref_map() -> Dict[str, str]:
    try:
        if USERREF_MAP_PATH.exists():
            data = json.loads(USERREF_MAP_PATH.read_text(encoding="utf-8"))
            # normalize to string->string
            out: Dict[str, str] = {}
            for k, v in (data or {}).items():
                out[str(k).strip()] = str(v).strip().lower()
            return out
    except Exception:
        pass
    return {}

def _iter_jsonl(path: Path, since_ts: float) -> Iterable[Dict[str, Any]]:
    if not path.exists():
        return
    try:
        with open(path, "r", encoding="utf-8") as fh:
            for line in fh:
                line = (line or "").strip()
                if not line:
                    continue
                try:
                    ev = json.loads(line)
                except Exception:
                    continue
                ts = _safe_float(ev.get("ts"), 0.0)
                if ts >= since_ts:
                    yield ev
    except Exception:
        return

def _db_conn() -> sqlite3.Connection:
    # app.py ensures schema, but keep defensive here
    con = sqlite3.connect(str(DB_PATH))
    return con

def _fetch_trades_since(since_ts: float) -> List[Dict[str, Any]]:
    if not DB_PATH.exists():
        return []
    con = _db_conn()
    cur = con.cursor()
    try:
        cur.execute(
            """
            SELECT txid, ts, pair, symbol, side, price, volume, cost, fee, strategy, raw
            FROM trades
            WHERE ts >= ?
            ORDER BY ts ASC
            """,
            (float(since_ts),),
        )
        rows = []
        for r in cur.fetchall():
            rows.append(
                {
                    "txid": r[0],
                    "ts": float(r[1] or 0),
                    "pair": r[2],
                    "symbol": _canon_symbol(r[3], r[2]),
                    "side": r[4],
                    "price": float(r[5] or 0),
                    "volume": float(r[6] or 0),
                    "cost": float(r[7] or 0),
                    "fee": float(r[8] or 0),
                    "strategy": r[9],
                    "raw": r[10],
                }
            )
        return rows
    finally:
        con.close()

def _infer_strategy(row: Dict[str, Any], userref_map: Dict[str, str]) -> str:
    s = (row.get("strategy") or "").strip().lower()
    if s:
        return s
    raw = row.get("raw")
    try:
        if isinstance(raw, str) and raw.strip():
            rawj = json.loads(raw)
        elif isinstance(raw, dict):
            rawj = raw
        else:
            rawj = {}
        u = rawj.get("userref") or rawj.get("user_ref") or rawj.get("userRef")
        if u is None and isinstance(rawj.get("result"), dict):
            u = rawj["result"].get("userref")
        if u is None:
            return "unknown"
        return userref_map.get(str(u).strip(), "unknown")
    except Exception:
        return "unknown"

def _bucket_hour(ts: float, bucket_hours: int = 4) -> str:
    # bucket in local time if TZ is set; otherwise UTC
    # We do not import pytz; use system tz handling via time.localtime.
    lt = time.localtime(ts)
    h = int(getattr(lt, "tm_hour", 0) or 0)
    start = (h // bucket_hours) * bucket_hours
    end = start + bucket_hours
    return f"{start:02d}-{end:02d}"

# -----------------------
# PnL + hold-time engine (FIFO lots w/ timestamps)
# -----------------------

@dataclass
class CloseChunk:
    strategy: str
    symbol: str
    pnl: float
    qty: float
    entry_ts: float
    exit_ts: float

@dataclass
class Lot:
    qty: float         # signed: >0 long, <0 short
    price: float       # entry price
    ts: float          # entry timestamp

def _compute_close_chunks(trades: List[Dict[str, Any]]) -> List[CloseChunk]:
    lots: Dict[Tuple[str, str], List[Lot]] = {}
    closes: List[CloseChunk] = []

    def push_lot(k: Tuple[str, str], lot: Lot) -> None:
        lots.setdefault(k, []).append(lot)

    def pop_lot(k: Tuple[str, str]) -> Optional[Lot]:
        arr = lots.get(k) or []
        if not arr:
            return None
        lot = arr[0]
        arr.pop(0)
        if not arr:
            lots.pop(k, None)
        return lot

    def peek_lot(k: Tuple[str, str]) -> Optional[Lot]:
        arr = lots.get(k) or []
        return arr[0] if arr else None

    for tr in trades:
        sym = _safe_str(tr.get("symbol")).strip()
        strat = _safe_str(tr.get("strategy")).strip().lower() or "unknown"
        side = _safe_str(tr.get("side")).strip().lower()
        px = _safe_float(tr.get("price"), 0.0)
        qty = _safe_float(tr.get("volume"), 0.0)
        ts = _safe_float(tr.get("ts"), 0.0)
        if not sym or not side or px <= 0.0 or qty <= 0.0:
            continue

        k = (strat, sym)

        # Convert to signed incoming quantity
        inc = qty if side == "buy" else -qty

        # While incoming opposes existing lots, close
        while abs(inc) > 1e-12:
            top = peek_lot(k)
            if top is None:
                break

            # If same direction, stop closing
            if (top.qty >= 0 and inc >= 0) or (top.qty <= 0 and inc <= 0):
                break

            lot = pop_lot(k)
            if lot is None:
                break

            # close quantity is limited by smaller magnitude
            close_qty = min(abs(inc), abs(lot.qty))

            # Realized PnL
            if lot.qty > 0 and inc < 0:
                # closing long with sell
                pnl = (px - lot.price) * close_qty
            elif lot.qty < 0 and inc > 0:
                # closing short with buy
                pnl = (lot.price - px) * close_qty
            else:
                pnl = 0.0

            closes.append(
                CloseChunk(
                    strategy=strat,
                    symbol=sym,
                    pnl=float(pnl),
                    qty=float(close_qty),
                    entry_ts=float(lot.ts),
                    exit_ts=float(ts),
                )
            )

            # Reduce lot and incoming
            if abs(lot.qty) > close_qty + 1e-12:
                # put back remaining lot
                rem_qty = abs(lot.qty) - close_qty
                rem_signed = rem_qty if lot.qty > 0 else -rem_qty
                push_lot(k, Lot(qty=rem_signed, price=lot.price, ts=lot.ts))

            # reduce incoming
            if inc > 0:
                inc -= close_qty
            else:
                inc += close_qty

        # Any remainder becomes a new lot
        if abs(inc) > 1e-12:
            push_lot(k, Lot(qty=float(inc), price=float(px), ts=float(ts)))

    return closes

def _max_drawdown(equity_curve: List[float]) -> float:
    if not equity_curve:
        return 0.0
    peak = equity_curve[0]
    max_dd = 0.0
    for x in equity_curve:
        peak = max(peak, x)
        dd = peak - x
        max_dd = max(max_dd, dd)
    return float(max_dd)

# -----------------------
# Aggregation
# -----------------------

def generate_report(
    hours: int = 24,
    days: Optional[int] = None,
    bucket_hours: int = 4,
    write_file: bool = True,
    out_name: str = "advisor_v2_report.json",
) -> Dict[str, Any]:
    """
    Full Advisor v2 report:
    - Trades (executed fills) from journal.db trades table
    - Scheduler intents & broker actions from journal_v2.jsonl
    - Guard/risk/skip telemetry from telemetry_v2.jsonl
    """
    now = _now_ts()
    span = (int(days) * 86400) if days else (int(hours) * 3600)
    since_ts = now - max(span, 60)

    userref_map = _load_userref_map()

    # Ingest
    journal_events = list(_iter_jsonl(JOURNAL_V2_PATH, since_ts))
    telemetry_rows = list(_iter_jsonl(TELEMETRY_V2_PATH, since_ts))
    trades = _fetch_trades_since(since_ts)

    # Normalize strategy on trades using userref_map if needed
    for tr in trades:
        tr["strategy"] = _infer_strategy(tr, userref_map)

    # Executed trades / PnL / hold time from fills
    closes = _compute_close_chunks(trades)

    # --- per-strategy and per-symbol aggregates ---
    per_strategy: Dict[str, Dict[str, Any]] = {}
    per_symbol: Dict[str, Dict[str, Any]] = {}
    per_pair: Dict[Tuple[str, str], Dict[str, Any]] = {}

    def bump(map_, key, field, amt=1):
        m = map_.setdefault(key, {})
        m[field] = m.get(field, 0) + amt
        return m

    # Count fills
    for tr in trades:
        strat = tr["strategy"]
        sym = tr.get("symbol")
        side = (tr.get("side") or "").lower()
        bump(per_strategy, strat, "fills", 1)
        bump(per_symbol, sym, "fills", 1)
        bump(per_pair, (strat, sym), "fills", 1)
        if side == "buy":
            bump(per_strategy, strat, "buys", 1)
            bump(per_symbol, sym, "buys", 1)
            bump(per_pair, (strat, sym), "buys", 1)
        elif side == "sell":
            bump(per_strategy, strat, "sells", 1)
            bump(per_symbol, sym, "sells", 1)
            bump(per_pair, (strat, sym), "sells", 1)

    # Close chunks => realized pnl, win/loss, hold time
    for c in closes:
        strat, sym = c.strategy, c.symbol
        pnl = c.pnl
        hold_s = max(0.0, c.exit_ts - c.entry_ts)
        # per strategy
        s = per_strategy.setdefault(strat, {})
        s["realized_pnl"] = s.get("realized_pnl", 0.0) + pnl
        s["closed_chunks"] = s.get("closed_chunks", 0) + 1
        s["hold_seconds_sum"] = s.get("hold_seconds_sum", 0.0) + hold_s
        s["hold_seconds_n"] = s.get("hold_seconds_n", 0) + 1
        if pnl > 0:
            s["wins"] = s.get("wins", 0) + 1
            s["win_sum"] = s.get("win_sum", 0.0) + pnl
        elif pnl < 0:
            s["losses"] = s.get("losses", 0) + 1
            s["loss_sum"] = s.get("loss_sum", 0.0) + pnl

        # per symbol
        sm = per_symbol.setdefault(sym, {})
        sm["realized_pnl"] = sm.get("realized_pnl", 0.0) + pnl
        sm["closed_chunks"] = sm.get("closed_chunks", 0) + 1
        sm["hold_seconds_sum"] = sm.get("hold_seconds_sum", 0.0) + hold_s
        sm["hold_seconds_n"] = sm.get("hold_seconds_n", 0) + 1
        if pnl > 0:
            sm["wins"] = sm.get("wins", 0) + 1
            sm["win_sum"] = sm.get("win_sum", 0.0) + pnl
        elif pnl < 0:
            sm["losses"] = sm.get("losses", 0) + 1
            sm["loss_sum"] = sm.get("loss_sum", 0.0) + pnl

        # per pair
        pm = per_pair.setdefault((strat, sym), {})
        pm["realized_pnl"] = pm.get("realized_pnl", 0.0) + pnl
        pm["closed_chunks"] = pm.get("closed_chunks", 0) + 1
        pm["hold_seconds_sum"] = pm.get("hold_seconds_sum", 0.0) + hold_s
        pm["hold_seconds_n"] = pm.get("hold_seconds_n", 0) + 1
        if pnl > 0:
            pm["wins"] = pm.get("wins", 0) + 1
            pm["win_sum"] = pm.get("win_sum", 0.0) + pnl
        elif pnl < 0:
            pm["losses"] = pm.get("losses", 0) + 1
            pm["loss_sum"] = pm.get("loss_sum", 0.0) + pnl

    # Drawdown per strategy and per pair (from realized equity curve)
    # We build equity series by processing close chunks in time order.
    closes_sorted = sorted(closes, key=lambda c: c.exit_ts)
    eq_by_strat: Dict[str, List[float]] = {}
    eq_by_pair: Dict[Tuple[str, str], List[float]] = {}
    cur_eq_strat: Dict[str, float] = {}
    cur_eq_pair: Dict[Tuple[str, str], float] = {}
    for c in closes_sorted:
        cur_eq_strat[c.strategy] = cur_eq_strat.get(c.strategy, 0.0) + c.pnl
        eq_by_strat.setdefault(c.strategy, []).append(cur_eq_strat[c.strategy])
        k = (c.strategy, c.symbol)
        cur_eq_pair[k] = cur_eq_pair.get(k, 0.0) + c.pnl
        eq_by_pair.setdefault(k, []).append(cur_eq_pair[k])

    for strat, curve in eq_by_strat.items():
        per_strategy.setdefault(strat, {})["max_drawdown"] = _max_drawdown(curve)
    for k, curve in eq_by_pair.items():
        per_pair.setdefault(k, {})["max_drawdown"] = _max_drawdown(curve)

    # Derived metrics
    def finalize(d: Dict[str, Any]) -> Dict[str, Any]:
        wins = int(d.get("wins", 0))
        losses = int(d.get("losses", 0))
        n = wins + losses
        d["win_rate"] = (wins / n) if n > 0 else None
        win_sum = _safe_float(d.get("win_sum"), 0.0)
        loss_sum = _safe_float(d.get("loss_sum"), 0.0)
        d["avg_win"] = (win_sum / wins) if wins > 0 else None
        d["avg_loss"] = (loss_sum / losses) if losses > 0 else None  # negative
        hs_n = int(d.get("hold_seconds_n", 0))
        hs_sum = _safe_float(d.get("hold_seconds_sum"), 0.0)
        d["avg_hold_seconds"] = (hs_sum / hs_n) if hs_n > 0 else None
        return d

    for k in list(per_strategy.keys()):
        per_strategy[k] = finalize(per_strategy[k])
    for k in list(per_symbol.keys()):
        per_symbol[k] = finalize(per_symbol[k])
    for k in list(per_pair.keys()):
        per_pair[k] = finalize(per_pair[k])

    # -----------------------
    # Telemetry attribution: blocks / skips / guards
    # -----------------------
    filter_counts: Dict[str, Dict[str, Any]] = {}
    strat_filter_counts: Dict[str, Dict[str, int]] = {}
    sym_filter_counts: Dict[str, Dict[str, int]] = {}
    time_buckets: Dict[str, Dict[str, Any]] = {}

    def norm_filter(reason: str) -> str:
        r = (reason or "").strip()
        if not r:
            return "unknown"
        rlow = r.lower()
        if rlow.startswith("guard_warn:"):
            inner = r.split(":", 1)[1]
            if "not_in_strategy_whitelist" in inner:
                return "whitelist_relaxed"
            return "guard_warn"
        if "not_in_strategy_whitelist" in rlow:
            return "whitelist"
        if "below_min_notional" in rlow or "min_notional" in rlow:
            return "min_notional"
        if "notional_after_caps" in rlow or "cap" in rlow:
            return "caps"
        if "loss_zone_no_rebuy" in rlow:
            return "loss_zone_no_rebuy"
        if "no_bars" in rlow:
            return "data_no_bars"
        if "no_scan_result" in rlow:
            return "no_scan_result"
        if "entry_without_notional" in rlow:
            return "entry_without_notional"
        if "insufficient" in rlow and "fund" in rlow:
            return "insufficient_funds"
        if "insufficient" in rlow and ("size" in rlow or "notional" in rlow):
            return "insufficient_size"
        if "atr" in rlow or "vol" in rlow or "volume" in rlow or "trend" in rlow or "mom" in rlow or "break" in rlow:
            return "signal_filter"
        return rlow[:60]

    for row in telemetry_rows:
        reason = _safe_str(row.get("reason"), "")
        if not reason:
            continue
        fkey = norm_filter(reason)
        strat = _safe_str(row.get("strategy"), "").lower() or "unknown"
        sym = _safe_str(row.get("symbol"), "") or "unknown"
        ts = _safe_float(row.get("ts"), 0.0) or since_ts

        filter_counts.setdefault(fkey, {"count": 0})
        filter_counts[fkey]["count"] += 1

        strat_filter_counts.setdefault(strat, {})
        strat_filter_counts[strat][fkey] = strat_filter_counts[strat].get(fkey, 0) + 1

        sym_filter_counts.setdefault(sym, {})
        sym_filter_counts[sym][fkey] = sym_filter_counts[sym].get(fkey, 0) + 1

        b = _bucket_hour(ts, bucket_hours=bucket_hours)
        tb = time_buckets.setdefault(b, {"count": 0, "realized_pnl": 0.0, "closed_chunks": 0, "filters": {}})
        tb["count"] += 1
        tb["filters"][fkey] = tb["filters"].get(fkey, 0) + 1

    # PnL by time bucket (using close chunks exit time)
    for c in closes_sorted:
        b = _bucket_hour(c.exit_ts, bucket_hours=bucket_hours)
        tb = time_buckets.setdefault(b, {"count": 0, "realized_pnl": 0.0, "closed_chunks": 0, "filters": {}})
        tb["realized_pnl"] = tb.get("realized_pnl", 0.0) + c.pnl
        tb["closed_chunks"] = tb.get("closed_chunks", 0) + 1

    # -----------------------
    # Detect issues
    # -----------------------
    issues: List[Dict[str, Any]] = []
    recs: List[Dict[str, Any]] = []

    # Strategy issues
    for strat, st in per_strategy.items():
        pnl = _safe_float(st.get("realized_pnl"), 0.0)
        closes_n = int(st.get("closed_chunks", 0))
        fills_n = int(st.get("fills", 0))
        blocks = sum(strat_filter_counts.get(strat, {}).values()) if strat in strat_filter_counts else 0
        # over/under filter heuristic
        if fills_n < 3 and blocks >= 25:
            issues.append({"type": "over_filtered", "strategy": strat, "fills": fills_n, "blocks": blocks})
            recs.append({
                "type": "relax_filters",
                "scope": "strategy",
                "strategy": strat,
                "why": "Many blocks vs few trades in window",
                "suggestions": ["Relax one gating filter at a time (ATR/volume/score) and re-check 24h attribution"]
            })
        if fills_n >= 20 and pnl < 0:
            issues.append({"type": "under_filtered_lossy", "strategy": strat, "fills": fills_n, "realized_pnl": pnl})
            recs.append({
                "type": "tighten_filters",
                "scope": "strategy",
                "strategy": strat,
                "why": "High activity with negative realized PnL",
                "suggestions": ["Increase entry score threshold or add stricter ATR/volume gates", "Reduce active hours for worst-performing buckets"]
            })
        if closes_n >= 5 and pnl < 0:
            recs.append({
                "type": "reduce_symbols",
                "scope": "strategy",
                "strategy": strat,
                "why": "Strategy negative in window",
                "suggestions": ["Drop consistently losing symbols for this strategy (see per_pair rankings)"]
            })

    # Symbol issues
    for sym, ss in per_symbol.items():
        pnl = _safe_float(ss.get("realized_pnl"), 0.0)
        fills_n = int(ss.get("fills", 0))
        if fills_n >= 20 and pnl < 0:
            issues.append({"type": "symbol_lossy_high_activity", "symbol": sym, "fills": fills_n, "realized_pnl": pnl})
            recs.append({
                "type": "disable_symbol",
                "scope": "symbol",
                "symbol": sym,
                "why": "High activity with negative realized PnL",
                "suggestions": ["Temporarily disable this symbol in worst strategy pairs", "Consider tighter volatility/volume gating for this symbol"]
            })

    # Whitelist-relaxed warnings
    wl_relaxed_total = filter_counts.get("whitelist_relaxed", {}).get("count", 0)
    if wl_relaxed_total:
        recs.append({
            "type": "whitelist_review",
            "scope": "global",
            "why": "Whitelist is relaxed and being hit in telemetry",
            "count": wl_relaxed_total,
            "suggestions": ["Use per_pair PnL to decide which non-whitelisted pairs to promote, and which to block/avoid"]
        })

    # Bad time buckets
    for b, tb in time_buckets.items():
        pnl = _safe_float(tb.get("realized_pnl"), 0.0)
        if int(tb.get("closed_chunks", 0)) >= 5 and pnl < 0:
            issues.append({"type": "bad_session_window", "bucket": b, "realized_pnl": pnl, "closed_chunks": tb.get("closed_chunks")})
            recs.append({
                "type": "adjust_active_hours",
                "scope": "global",
                "bucket": b,
                "why": "Negative PnL concentration in this time window",
                "suggestions": ["Reduce active hours during this bucket for the worst strategies", "Increase filters during this bucket only (time-gated filter tiers)"]
            })

    # -----------------------
    # Build report
    # -----------------------
    per_pair_list = []
    for (strat, sym), v in per_pair.items():
        row = {"strategy": strat, "symbol": sym, **v}
        per_pair_list.append(row)
    per_pair_list.sort(key=lambda r: _safe_float(r.get("realized_pnl"), 0.0))

    per_strategy_list = [{"strategy": k, **v} for k, v in per_strategy.items()]
    per_strategy_list.sort(key=lambda r: _safe_float(r.get("realized_pnl"), 0.0))

    per_symbol_list = [{"symbol": k, **v} for k, v in per_symbol.items()]
    per_symbol_list.sort(key=lambda r: _safe_float(r.get("realized_pnl"), 0.0))

    report = {
        "ok": True,
        "window": {
            "since_ts": since_ts,
            "until_ts": now,
            "hours": hours,
            "days": days,
        },
        "inputs": {
            "journal_v2_path": str(JOURNAL_V2_PATH),
            "telemetry_v2_path": str(TELEMETRY_V2_PATH),
            "db_path": str(DB_PATH),
            "userref_map_path": str(USERREF_MAP_PATH),
        },
        "counts": {
            "journal_v2_events": len(journal_events),
            "telemetry_rows": len(telemetry_rows),
            "trades_fills": len(trades),
            "closed_chunks": len(closes),
        },
        "per_strategy": per_strategy_list,
        "per_symbol": per_symbol_list,
        "per_pair": per_pair_list,
        "per_filter": {k: v for k, v in sorted(filter_counts.items(), key=lambda kv: kv[1].get("count", 0), reverse=True)},
        "per_strategy_filter": strat_filter_counts,
        "per_symbol_filter": sym_filter_counts,
        "per_time_bucket": time_buckets,
        "issues": issues,
        "recommendations": recs,
    }

    if write_file:
        try:
            DATA_DIR.mkdir(parents=True, exist_ok=True)
            out_path = DATA_DIR / out_name
            with open(out_path, "w", encoding="utf-8") as fh:
                json.dump(report, fh, indent=2)
            report["output_path"] = str(out_path)
        except Exception as e:
            report["output_error"] = str(e)

    return report


def to_markdown(report: Dict[str, Any], top_n: int = 10) -> str:
    if not report or not report.get("ok"):
        return f"Advisor v2 failed: {report}"
    win = report.get("window", {})
    counts = report.get("counts", {})
    md = []
    md.append(f"# Advisor v2 Report\n")
    md.append(f"Window: since_ts={win.get('since_ts'):.0f} until_ts={win.get('until_ts'):.0f} (hours={win.get('hours')}, days={win.get('days')})\n")
    md.append(f"Counts: trades_fills={counts.get('trades_fills')} closed_chunks={counts.get('closed_chunks')} telemetry_rows={counts.get('telemetry_rows')} journal_v2_events={counts.get('journal_v2_events')}\n")

    def section(title: str):
        md.append(f"\n## {title}\n")

    section("Top Losing Strategies (Realized PnL)")
    rows = report.get("per_strategy", [])[:]
    rows.sort(key=lambda r: float(r.get("realized_pnl") or 0.0))
    for r in rows[:top_n]:
        md.append(f"- **{r.get('strategy')}** pnl={r.get('realized_pnl'):.2f} win_rate={r.get('win_rate')} avg_hold_s={r.get('avg_hold_seconds')}\n")

    section("Top Losing Pairs (Strategy × Symbol)")
    pairs = report.get("per_pair", [])[:]
    for r in pairs[:top_n]:
        md.append(f"- **{r.get('strategy')} / {r.get('symbol')}** pnl={r.get('realized_pnl'):.2f} fills={r.get('fills',0)} win_rate={r.get('win_rate')}\n")

    section("Filter / Block Hotspots")
    pf = report.get("per_filter", {})
    for k, v in list(pf.items())[:top_n]:
        md.append(f"- **{k}** count={v.get('count')}\n")

    section("Bad Time Buckets (PnL)")
    tb = report.get("per_time_bucket", {})
    items = []
    for b, v in tb.items():
        pnl = float(v.get("realized_pnl") or 0.0)
        if int(v.get("closed_chunks", 0)) >= 3:
            items.append((pnl, b, v))
    items.sort(key=lambda x: x[0])
    for pnl, b, v in items[:top_n]:
        md.append(f"- **{b}** pnl={pnl:.2f} closed_chunks={v.get('closed_chunks')}\n")

    section("Recommendations")
    for rec in report.get("recommendations", [])[:top_n]:
        md.append(f"- **{rec.get('type')}** scope={rec.get('scope')} {rec}\n")

    return "".join(md)


def summarize_v2(limit: int = 1000) -> Dict[str, Any]:
    """Backward-compatible endpoint payload: activity counts over recent journal_v2 entries."""
    now = _now_ts()
    since_ts = now - 24*3600
    events = list(_iter_jsonl(JOURNAL_V2_PATH, since_ts))
    # truncate oldest if needed
    if limit and len(events) > limit:
        events = events[-limit:]
    per_strategy: Dict[str, int] = {}
    per_pair: Dict[str, int] = {}
    for ev in events:
        s = _safe_str(ev.get('strategy'), 'unknown').lower() or 'unknown'
        sym = _safe_str(ev.get('symbol'), 'unknown')
        per_strategy[s] = per_strategy.get(s, 0) + 1
        k = f"{s}:{sym}"
        per_pair[k] = per_pair.get(k, 0) + 1
    top_strats = sorted(per_strategy.items(), key=lambda kv: kv[1], reverse=True)[:10]
    top_pairs = sorted(per_pair.items(), key=lambda kv: kv[1], reverse=True)[:10]
    return {
        'ok': True,
        'count': len(events),
        'limit': limit,
        'top_strategies': [{'strategy': k, 'count': v} for k,v in top_strats],
        'top_pairs': [{'pair': k, 'count': v} for k,v in top_pairs],
    }

# Backwards-compatible alias (app.py imports advisor_summary)
def advisor_summary(limit: int = 1000):
    return summarize_v2(limit=limit)
