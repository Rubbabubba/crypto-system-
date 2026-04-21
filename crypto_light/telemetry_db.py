from __future__ import annotations

import json
import os
import sqlite3
import time
from typing import Any, Dict, List, Optional

from . import trade_journal
from . import lifecycle_db


def _canonical_trade_symbol(symbol: str) -> str:
    s = str(symbol or '').strip().upper()
    if not s:
        return ''
    compact = s.replace('/', '').replace('-', '').replace('_', '')
    for q in ('USDT', 'USDC', 'USD', 'EUR'):
        if compact.endswith(q):
            base = compact[:-len(q)]
            alias_map = {'XXBT': 'BTC', 'XBT': 'BTC', 'XXBTZ': 'BTC', 'XBTZ': 'BTC', 'ZXXBT': 'BTC', 'ZXBT': 'BTC'}
            base = alias_map.get(base, base)
            if base == 'XBT':
                base = 'BTC'
            return f'{base}/{q}'
    return s


def _journal_row_key(row: Dict[str, Any]) -> str:
    exit_txid = str(row.get('exit_txid') or '').strip()
    if exit_txid:
        return f'exit:{exit_txid}'
    return '|'.join([
        _canonical_trade_symbol(str(row.get('symbol') or '')),
        str(row.get('opened_ts') or ''),
        str(row.get('closed_ts') or ''),
        str(row.get('entry_txid') or ''),
        str(row.get('exit_txid') or ''),
        str(row.get('entry_qty') or ''),
        str(row.get('exit_qty') or ''),
    ])


def _dedupe_journal_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = set()
    out: List[Dict[str, Any]] = []
    for row in rows:
        key = _journal_row_key(row)
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out


def _db_path() -> str:
    return os.getenv("TELEMETRY_DB_PATH", "/var/data/telemetry.sqlite3")


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(_db_path())
    conn.row_factory = sqlite3.Row
    return conn


def _ensure_columns(conn: sqlite3.Connection, table: str, cols: Dict[str, str]) -> None:
    existing = {str(r[1]) for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    for name, ctype in cols.items():
        if name not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {ctype}")


def init_db() -> None:
    with _connect() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS trade_telemetry (
              trade_key TEXT PRIMARY KEY,
              symbol TEXT,
              strategy TEXT,
              source TEXT,
              signal_name TEXT,
              signal_id TEXT,
              req_id TEXT,
              opened_ts REAL,
              closed_ts REAL,
              hold_sec REAL,
              entry_txid TEXT,
              exit_txid TEXT,
              entry_execution TEXT,
              exit_execution TEXT,
              entry_execution_path TEXT,
              exit_execution_path TEXT,
              entry_liquidity_role TEXT,
              exit_liquidity_role TEXT,
              entry_ref_price REAL,
              exit_ref_price REAL,
              entry_fill_price REAL,
              exit_fill_price REAL,
              entry_qty REAL,
              exit_qty REAL,
              entry_cost REAL,
              exit_cost REAL,
              entry_fee REAL,
              exit_fee REAL,
              entry_fee_bps REAL,
              exit_fee_bps REAL,
              entry_realized_slippage_bps REAL,
              exit_realized_slippage_bps REAL,
              max_realized_slippage_bps REAL,
              projected_move_bps REAL,
              projected_cost_bps REAL,
              realized_move_bps REAL,
              realized_cost_bps REAL,
              net_edge_bps REAL,
              projected_move_to_cost_mult REAL,
              realized_move_to_cost_mult REAL,
              hold_bucket TEXT,
              regime_state TEXT,
              expectancy_bucket TEXT,
              fees_total REAL,
              gross_pnl_usd REAL,
              net_pnl_usd REAL,
              exit_reason TEXT,
              partial_close INTEGER,
              clean_trade INTEGER,
              alert_flags_json TEXT,
              meta_json TEXT,
              created_utc REAL,
              updated_utc REAL
            )
            """
        )
        _ensure_columns(conn, 'trade_telemetry', {
            'entry_execution_path': 'TEXT',
            'exit_execution_path': 'TEXT',
            'entry_liquidity_role': 'TEXT',
            'exit_liquidity_role': 'TEXT',
            'projected_move_bps': 'REAL',
            'projected_cost_bps': 'REAL',
            'realized_move_bps': 'REAL',
            'realized_cost_bps': 'REAL',
            'net_edge_bps': 'REAL',
            'projected_move_to_cost_mult': 'REAL',
            'realized_move_to_cost_mult': 'REAL',
            'hold_bucket': 'TEXT',
            'regime_state': 'TEXT',
            'expectancy_bucket': 'TEXT',
        })
        conn.execute("CREATE INDEX IF NOT EXISTS idx_trade_telemetry_closed_ts ON trade_telemetry(closed_ts DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_trade_telemetry_symbol_closed_ts ON trade_telemetry(symbol, closed_ts DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_trade_telemetry_regime_closed_ts ON trade_telemetry(regime_state, closed_ts DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_trade_telemetry_hold_bucket_closed_ts ON trade_telemetry(hold_bucket, closed_ts DESC)")
        conn.commit()


def _to_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        return float(x)
    except Exception:
        return None


def _safe_json_loads(raw: Any) -> Dict[str, Any]:
    if isinstance(raw, dict):
        return dict(raw)
    if not raw:
        return {}
    try:
        val = json.loads(raw)
        return dict(val) if isinstance(val, dict) else {}
    except Exception:
        return {}


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)) or default)
    except Exception:
        return float(default)


def _env_int(name: str, default: int) -> int:
    try:
        return int(float(os.getenv(name, str(default)) or default))
    except Exception:
        return int(default)


def _trade_key(row: Dict[str, Any]) -> str:
    return _journal_row_key(row)


def _slippage_bps(fill_px: Optional[float], ref_px: Optional[float]) -> Optional[float]:
    if fill_px is None or ref_px is None:
        return None
    if ref_px <= 0:
        return None
    return abs(float(fill_px) - float(ref_px)) / float(ref_px) * 10000.0


def _fee_bps(fee: Optional[float], cost: Optional[float]) -> Optional[float]:
    if fee is None or cost is None:
        return None
    if cost <= 0:
        return None
    return abs(float(fee)) / abs(float(cost)) * 10000.0


def _liquidity_role(execution: Any, order_result: Dict[str, Any] | None = None) -> str:
    return _execution_detail(execution, order_result).get('liquidity_role') or 'unknown'


def _execution_path(execution: Any, order_result: Dict[str, Any] | None = None) -> str:
    return _execution_detail(execution, order_result).get('path') or 'unknown'

def _hold_bucket(hold_sec: Optional[float]) -> str:
    hs = _to_float(hold_sec)
    if hs is None:
        return 'unknown'
    if hs < 900:
        return '<15m'
    if hs < 3600:
        return '15m-1h'
    if hs < 4 * 3600:
        return '1h-4h'
    if hs < 24 * 3600:
        return '4h-24h'
    return '24h+'


def _normalize_regime_label(value: Any) -> str:
    s = str(value or '').strip().lower()
    if not s:
        return 'unknown'
    mapping = {
        'quiet': 'quiet',
        'expansion': 'expansion',
        'trend': 'expansion',
        'trending': 'expansion',
        'chop': 'quiet',
        'choppy': 'quiet',
        'range': 'quiet',
        'ranging': 'quiet',
        'unknown': 'unknown',
    }
    return mapping.get(s, s)


def _resolve_regime_state(entry_ctx: Dict[str, Any], exit_ctx: Dict[str, Any], meta: Dict[str, Any]) -> str:
    for candidate in (
        entry_ctx.get('regime_state'),
        meta.get('regime_state'),
        ((entry_ctx.get('signal_meta') or {}).get('regime') or {}).get('state'),
        ((entry_ctx.get('signal_meta') or {}).get('regime') or {}).get('reason'),
        ((entry_ctx.get('extra') or {}).get('signal_meta') or {}).get('regime_state'),
        exit_ctx.get('regime_state'),
    ):
        label = _normalize_regime_label(candidate)
        if label != 'unknown':
            return label
    for flag in (
        entry_ctx.get('regime_quiet'),
        ((entry_ctx.get('signal_meta') or {}).get('regime') or {}).get('regime_quiet'),
        ((entry_ctx.get('extra') or {}).get('regime_quiet') if isinstance(entry_ctx.get('extra'), dict) else None),
        meta.get('regime_quiet'),
        exit_ctx.get('regime_quiet'),
    ):
        if flag is True:
            return 'quiet'
        if flag is False:
            return 'expansion'
    return 'unknown'


def _execution_detail(execution: Any, order_result: Dict[str, Any] | None = None) -> Dict[str, str]:
    s = str(execution or '').strip().lower()
    res = dict(order_result or {})
    if not s and res:
        s = str(res.get('execution') or '').strip().lower()
    if res.get('maker_first'):
        if s == 'market':
            return {'path': 'maker_first_fallback_market', 'liquidity_role': 'taker'}
        if 'post_only' in s:
            return {'path': 'maker_first_post_only', 'liquidity_role': 'maker'}
        return {'path': 'maker_first', 'liquidity_role': 'mixed'}
    if res.get('aggressive_limit_first'):
        if s == 'market':
            return {'path': 'limit_aggressive_fallback_market', 'liquidity_role': 'taker'}
        return {'path': 'limit_aggressive', 'liquidity_role': 'taker'}
    if s in ('post_only_limit', 'maker', 'post_only'):
        return {'path': 'post_only', 'liquidity_role': 'maker'}
    if s in ('market', 'stop_loss', 'stop-limit', 'stop_limit'):
        return {'path': 'market' if s == 'market' else 'stop', 'liquidity_role': 'taker'}
    if s == 'limit_aggressive':
        return {'path': 'limit_aggressive', 'liquidity_role': 'taker'}
    if 'reconciled' in s or 'backfill' in s or 'reconstructed' in s or s in ('adopted', 'dry_run'):
        return {'path': s or 'unknown', 'liquidity_role': 'unknown'}
    if 'limit' in s:
        return {'path': 'limit', 'liquidity_role': 'unknown'}
    return {'path': s or 'unknown', 'liquidity_role': 'unknown'}


def _symbol_truth_summary(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        symbol = str(r.get('symbol') or 'unknown')
        cur = out.setdefault(symbol, {'trades': 0, 'wins': 0, 'losses': 0, 'net_pnl_usd': 0.0, 'avg_net_edge_bps': None})
        cur['trades'] += 1
        pnl = float(r.get('net_pnl_usd') or 0.0)
        cur['net_pnl_usd'] += pnl
        if pnl > 0:
            cur['wins'] += 1
        elif pnl < 0:
            cur['losses'] += 1
        cur.setdefault('_ne', []).append(_to_float(r.get('net_edge_bps')))
    for cur in out.values():
        ne = [float(x) for x in cur.pop('_ne', []) if x is not None]
        cur['win_rate'] = (float(cur['wins']) / float(cur['wins'] + cur['losses'])) if (cur['wins'] + cur['losses']) > 0 else None
        cur['avg_net_edge_bps'] = (sum(ne) / len(ne)) if ne else None
    return dict(sorted(out.items(), key=lambda kv: (-int(kv[1].get('trades') or 0), kv[0])))


def _execution_truth_summary(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    out = {'entry_paths': {}, 'exit_paths': {}, 'entry_roles': {}, 'exit_roles': {}, 'path_pairs': {}}
    for r in rows:
        ep = str(r.get('entry_execution_path') or 'unknown')
        xp = str(r.get('exit_execution_path') or 'unknown')
        er = str(r.get('entry_liquidity_role') or 'unknown')
        xr = str(r.get('exit_liquidity_role') or 'unknown')
        pair = f'{ep} -> {xp}'
        for bucket, key in ((out['entry_paths'], ep), (out['exit_paths'], xp), (out['entry_roles'], er), (out['exit_roles'], xr), (out['path_pairs'], pair)):
            bucket[key] = int(bucket.get(key, 0)) + 1
    return out


def _strategy_truth_summary(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    strategies = _group_stats(rows, 'strategy')
    symbols = _symbol_truth_summary(rows)
    regime_known = sum(1 for r in rows if str(r.get('regime_state') or 'unknown') != 'unknown')
    return {
        'by_strategy': strategies,
        'by_symbol': symbols,
        'regime_coverage': {
            'known': regime_known,
            'unknown': max(0, len(rows) - regime_known),
            'coverage_pct': (float(regime_known) / float(len(rows)) * 100.0) if rows else None,
        },
    }


def _bps_change(start_px: Optional[float], end_px: Optional[float]) -> Optional[float]:
    if start_px is None or end_px is None:
        return None
    if float(start_px) <= 0:
        return None
    return (float(end_px) - float(start_px)) / float(start_px) * 10000.0


def _ratio(num: Optional[float], den: Optional[float]) -> Optional[float]:
    if num is None or den is None:
        return None
    if float(den) <= 0:
        return None
    return float(num) / float(den)


def _expectancy_bucket(net_edge_bps: Optional[float]) -> str:
    v = _to_float(net_edge_bps)
    if v is None:
        return 'unknown'
    if v >= 50:
        return 'strong_positive'
    if v >= 0:
        return 'positive'
    if v >= -50:
        return 'slightly_negative'
    return 'negative'


def _group_stats(rows: List[Dict[str, Any]], key: str) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        bucket = str(r.get(key) or 'unknown')
        cur = out.setdefault(bucket, {'trades': 0, 'wins': 0, 'losses': 0, 'net_pnl_usd': 0.0, 'gross_pnl_usd': 0.0, 'fees_total_usd': 0.0, 'avg_realized_move_bps': None, 'avg_net_edge_bps': None})
        cur['trades'] += 1
        pnl = float(r.get('net_pnl_usd') or 0.0)
        gpnl = float(r.get('gross_pnl_usd') or 0.0)
        fees = float(r.get('fees_total') or 0.0)
        cur['net_pnl_usd'] += pnl
        cur['gross_pnl_usd'] += gpnl
        cur['fees_total_usd'] += fees
        if pnl > 0:
            cur['wins'] += 1
        elif pnl < 0:
            cur['losses'] += 1
        cur.setdefault('_rm', []).append(_to_float(r.get('realized_move_bps')))
        cur.setdefault('_ne', []).append(_to_float(r.get('net_edge_bps')))
    for bucket, cur in out.items():
        rm = [float(x) for x in cur.pop('_rm', []) if x is not None]
        ne = [float(x) for x in cur.pop('_ne', []) if x is not None]
        cur['win_rate'] = (float(cur['wins']) / float(cur['wins'] + cur['losses'])) if (cur['wins'] + cur['losses']) > 0 else None
        cur['avg_realized_move_bps'] = (sum(rm) / len(rm)) if rm else None
        cur['avg_net_edge_bps'] = (sum(ne) / len(ne)) if ne else None
    return out


def _blocked_trade_summary(days: float) -> Dict[str, Any]:
    since_ts = time.time() - max(0.0, float(days)) * 86400.0
    try:
        return lifecycle_db.summarize_admission_events(since_ts=since_ts)
    except Exception:
        return {'total': 0, 'by_result': {}, 'by_reason': {}}


def sync_from_trade_journal(limit: int = 500) -> Dict[str, Any]:
    init_db()
    rows = _dedupe_journal_rows(trade_journal.list_closed_trades(limit=max(1, int(limit))))
    expected_slippage_bps = _env_float("EXPECTED_SLIPPAGE_BPS", _env_float("SLIPPAGE_BPS", 8.0))
    entry_fee_bps_env = _env_float("ENTRY_FEE_BPS", 0.0)
    exit_fee_bps_env = _env_float("EXIT_FEE_BPS", 0.0)
    max_realized_slippage_bps_alert = _env_float("MAX_REALIZED_SLIPPAGE_BPS_ALERT", 35.0)
    max_entry_fee_bps_alert = _env_float("MAX_ENTRY_FEE_BPS_ALERT", 40.0)
    max_exit_fee_bps_alert = _env_float("MAX_EXIT_FEE_BPS_ALERT", 40.0)
    inserted = 0
    with _connect() as conn:
        conn.execute("DELETE FROM trade_telemetry")
        now_ts = time.time()
        for r in rows:
            meta = _safe_json_loads(r.get("meta_json"))
            order_result = _safe_json_loads(meta.get("order_result"))
            entry_ctx = _safe_json_loads(meta.get("entry_context"))
            exit_ctx = _safe_json_loads(meta.get("exit_context"))
            entry_ref_price = _to_float(meta.get("entry_ref_price"))
            if entry_ref_price is None:
                entry_ref_price = _to_float(entry_ctx.get("entry_ref_price", entry_ctx.get('reference_price')))
            exit_ref_price = _to_float(meta.get("exit_ref_price"))
            if exit_ref_price is None:
                exit_ref_price = _to_float(exit_ctx.get('exit_ref_price', exit_ctx.get('reference_price')))
            partial_close = bool(meta.get("partial_close") or False)
            entry_fill_price = _to_float(r.get("entry_price"))
            exit_fill_price = _to_float(r.get("exit_price"))
            entry_fee_bps = _fee_bps(_to_float(r.get("entry_fee")), _to_float(r.get("entry_cost")))
            exit_fee_bps = _fee_bps(_to_float(r.get("exit_fee")), _to_float(r.get("exit_cost")))
            entry_slip = _slippage_bps(entry_fill_price, entry_ref_price)
            exit_slip = _slippage_bps(exit_fill_price, exit_ref_price)
            slips = [x for x in [entry_slip, exit_slip] if x is not None]
            max_slip = max(slips) if slips else None
            projected_move_bps = _to_float(entry_ctx.get('expected_move_bps', meta.get('expected_move_bps')))
            projected_cost_bps = _to_float((entry_ctx.get('profit_filter') or {}).get('round_trip_cost_bps', meta.get('projected_cost_bps')))
            if projected_cost_bps is None:
                projected_cost_bps = float(entry_fee_bps_env) + float(exit_fee_bps_env) + (2.0 * float(expected_slippage_bps))
            realized_move_bps = _bps_change(entry_fill_price or entry_ref_price, exit_fill_price or exit_ref_price)
            realized_cost_bps = None
            if entry_fee_bps is not None or exit_fee_bps is not None:
                realized_cost_bps = float(entry_fee_bps or 0.0) + float(exit_fee_bps or 0.0) + float(entry_slip or 0.0) + float(exit_slip or 0.0)
            net_edge_bps = None if realized_move_bps is None or realized_cost_bps is None else float(realized_move_bps) - float(realized_cost_bps)
            projected_move_to_cost_mult = _ratio(projected_move_bps, projected_cost_bps)
            realized_move_to_cost_mult = _ratio(realized_move_bps, realized_cost_bps)
            regime_state = _resolve_regime_state(entry_ctx, exit_ctx, meta)
            entry_execution = r.get('entry_execution')
            exit_execution = r.get('exit_execution')
            entry_execution_path = _execution_path(entry_execution, order_result)
            exit_execution_path = _execution_path(exit_execution, order_result)
            entry_liquidity_role = _liquidity_role(entry_execution, order_result)
            exit_liquidity_role = _liquidity_role(exit_execution, order_result)
            alerts: List[str] = []
            if entry_slip is not None and entry_slip > max_realized_slippage_bps_alert:
                alerts.append("entry_slippage_high")
            if exit_slip is not None and exit_slip > max_realized_slippage_bps_alert:
                alerts.append("exit_slippage_high")
            if entry_fee_bps is not None and entry_fee_bps > max_entry_fee_bps_alert:
                alerts.append("entry_fee_high")
            if exit_fee_bps is not None and exit_fee_bps > max_exit_fee_bps_alert:
                alerts.append("exit_fee_high")
            if partial_close:
                alerts.append("partial_close")
            if net_edge_bps is not None and net_edge_bps < 0:
                alerts.append('negative_edge')
            clean_trade = int(len(alerts) == 0 and bool(r.get("exit_txid")) and bool(r.get("entry_txid")))
            payload = {
                "trade_key": _trade_key(r),
                "symbol": _canonical_trade_symbol(str(r.get("symbol") or "")),
                "strategy": r.get("strategy"),
                "source": r.get("source"),
                "signal_name": r.get("signal_name"),
                "signal_id": r.get("signal_id"),
                "req_id": r.get("req_id"),
                "opened_ts": _to_float(r.get("opened_ts")),
                "closed_ts": _to_float(r.get("closed_ts")),
                "hold_sec": _to_float(r.get("hold_sec")),
                "entry_txid": r.get("entry_txid"),
                "exit_txid": r.get("exit_txid"),
                "entry_execution": entry_execution,
                "exit_execution": exit_execution,
                "entry_execution_path": entry_execution_path,
                "exit_execution_path": exit_execution_path,
                "entry_liquidity_role": entry_liquidity_role,
                "exit_liquidity_role": exit_liquidity_role,
                "entry_ref_price": entry_ref_price,
                "exit_ref_price": exit_ref_price,
                "entry_fill_price": entry_fill_price,
                "exit_fill_price": exit_fill_price,
                "entry_qty": _to_float(r.get("entry_qty")),
                "exit_qty": _to_float(r.get("exit_qty")),
                "entry_cost": _to_float(r.get("entry_cost")),
                "exit_cost": _to_float(r.get("exit_cost")),
                "entry_fee": _to_float(r.get("entry_fee")),
                "exit_fee": _to_float(r.get("exit_fee")),
                "entry_fee_bps": entry_fee_bps,
                "exit_fee_bps": exit_fee_bps,
                "entry_realized_slippage_bps": entry_slip,
                "exit_realized_slippage_bps": exit_slip,
                "max_realized_slippage_bps": max_slip,
                "projected_move_bps": projected_move_bps,
                "projected_cost_bps": projected_cost_bps,
                "realized_move_bps": realized_move_bps,
                "realized_cost_bps": realized_cost_bps,
                "net_edge_bps": net_edge_bps,
                "projected_move_to_cost_mult": projected_move_to_cost_mult,
                "realized_move_to_cost_mult": realized_move_to_cost_mult,
                "hold_bucket": _hold_bucket(_to_float(r.get('hold_sec'))),
                "regime_state": regime_state,
                "expectancy_bucket": _expectancy_bucket(net_edge_bps),
                "fees_total": _to_float(r.get("fees_total")),
                "gross_pnl_usd": _to_float(r.get("gross_pnl_usd")),
                "net_pnl_usd": _to_float(r.get("net_pnl_usd")),
                "exit_reason": r.get("exit_reason"),
                "partial_close": 1 if partial_close else 0,
                "clean_trade": clean_trade,
                "alert_flags_json": json.dumps(alerts, separators=(",", ":")),
                "meta_json": json.dumps({
                    "expected_slippage_bps": expected_slippage_bps,
                    "order_result": order_result,
                    "entry_context": entry_ctx,
                    "exit_context": exit_ctx,
                    "source_meta": meta,
                    "synced_utc": now_ts,
                }, separators=(",", ":"), sort_keys=True),
                "created_utc": now_ts,
                "updated_utc": now_ts,
            }
            conn.execute(
                """
                INSERT INTO trade_telemetry (
                  trade_key, symbol, strategy, source, signal_name, signal_id, req_id, opened_ts, closed_ts, hold_sec,
                  entry_txid, exit_txid, entry_execution, exit_execution, entry_execution_path, exit_execution_path,
                  entry_liquidity_role, exit_liquidity_role, entry_ref_price, exit_ref_price,
                  entry_fill_price, exit_fill_price, entry_qty, exit_qty, entry_cost, exit_cost, entry_fee, exit_fee,
                  entry_fee_bps, exit_fee_bps, entry_realized_slippage_bps, exit_realized_slippage_bps, max_realized_slippage_bps,
                  projected_move_bps, projected_cost_bps, realized_move_bps, realized_cost_bps, net_edge_bps,
                  projected_move_to_cost_mult, realized_move_to_cost_mult, hold_bucket, regime_state, expectancy_bucket,
                  fees_total, gross_pnl_usd, net_pnl_usd, exit_reason, partial_close, clean_trade, alert_flags_json, meta_json, created_utc, updated_utc
                ) VALUES (
                  :trade_key, :symbol, :strategy, :source, :signal_name, :signal_id, :req_id, :opened_ts, :closed_ts, :hold_sec,
                  :entry_txid, :exit_txid, :entry_execution, :exit_execution, :entry_execution_path, :exit_execution_path,
                  :entry_liquidity_role, :exit_liquidity_role, :entry_ref_price, :exit_ref_price,
                  :entry_fill_price, :exit_fill_price, :entry_qty, :exit_qty, :entry_cost, :exit_cost, :entry_fee, :exit_fee,
                  :entry_fee_bps, :exit_fee_bps, :entry_realized_slippage_bps, :exit_realized_slippage_bps, :max_realized_slippage_bps,
                  :projected_move_bps, :projected_cost_bps, :realized_move_bps, :realized_cost_bps, :net_edge_bps,
                  :projected_move_to_cost_mult, :realized_move_to_cost_mult, :hold_bucket, :regime_state, :expectancy_bucket,
                  :fees_total, :gross_pnl_usd, :net_pnl_usd, :exit_reason, :partial_close, :clean_trade, :alert_flags_json, :meta_json, :created_utc, :updated_utc
                )
                ON CONFLICT(trade_key) DO UPDATE SET
                  strategy=excluded.strategy,
                  source=excluded.source,
                  signal_name=excluded.signal_name,
                  signal_id=excluded.signal_id,
                  req_id=excluded.req_id,
                  hold_sec=excluded.hold_sec,
                  entry_execution=excluded.entry_execution,
                  exit_execution=excluded.exit_execution,
                  entry_execution_path=excluded.entry_execution_path,
                  exit_execution_path=excluded.exit_execution_path,
                  entry_liquidity_role=excluded.entry_liquidity_role,
                  exit_liquidity_role=excluded.exit_liquidity_role,
                  entry_ref_price=excluded.entry_ref_price,
                  exit_ref_price=excluded.exit_ref_price,
                  entry_fill_price=excluded.entry_fill_price,
                  exit_fill_price=excluded.exit_fill_price,
                  entry_qty=excluded.entry_qty,
                  exit_qty=excluded.exit_qty,
                  entry_cost=excluded.entry_cost,
                  exit_cost=excluded.exit_cost,
                  entry_fee=excluded.entry_fee,
                  exit_fee=excluded.exit_fee,
                  entry_fee_bps=excluded.entry_fee_bps,
                  exit_fee_bps=excluded.exit_fee_bps,
                  entry_realized_slippage_bps=excluded.entry_realized_slippage_bps,
                  exit_realized_slippage_bps=excluded.exit_realized_slippage_bps,
                  max_realized_slippage_bps=excluded.max_realized_slippage_bps,
                  projected_move_bps=excluded.projected_move_bps,
                  projected_cost_bps=excluded.projected_cost_bps,
                  realized_move_bps=excluded.realized_move_bps,
                  realized_cost_bps=excluded.realized_cost_bps,
                  net_edge_bps=excluded.net_edge_bps,
                  projected_move_to_cost_mult=excluded.projected_move_to_cost_mult,
                  realized_move_to_cost_mult=excluded.realized_move_to_cost_mult,
                  hold_bucket=excluded.hold_bucket,
                  regime_state=excluded.regime_state,
                  expectancy_bucket=excluded.expectancy_bucket,
                  fees_total=excluded.fees_total,
                  gross_pnl_usd=excluded.gross_pnl_usd,
                  net_pnl_usd=excluded.net_pnl_usd,
                  exit_reason=excluded.exit_reason,
                  partial_close=excluded.partial_close,
                  clean_trade=excluded.clean_trade,
                  alert_flags_json=excluded.alert_flags_json,
                  meta_json=excluded.meta_json,
                  updated_utc=excluded.updated_utc
                """,
                payload,
            )
            inserted += 1
        conn.commit()
    return {"ok": True, "db_path": _db_path(), "synced_rows": inserted, "journal_rows": len(rows)}


def recent_trades(limit: int = 100) -> List[Dict[str, Any]]:
    init_db()
    sync_from_trade_journal(limit=max(limit, 200))
    with _connect() as conn:
        rows = conn.execute(
            "SELECT * FROM trade_telemetry ORDER BY closed_ts DESC LIMIT ?",
            (max(1, int(limit)),),
        ).fetchall()
    return [dict(r) for r in rows]


def summary(days: float = 30.0) -> Dict[str, Any]:
    init_db()
    sync_from_trade_journal(limit=5000)
    since = time.time() - max(0.0, float(days)) * 86400.0
    with _connect() as conn:
        rows = conn.execute(
            "SELECT * FROM trade_telemetry WHERE closed_ts >= ? ORDER BY closed_ts DESC",
            (since,),
        ).fetchall()
    rec = [dict(r) for r in rows]
    total = len(rec)
    clean = sum(int(r.get("clean_trade") or 0) for r in rec)
    slippage_vals = [float(r["max_realized_slippage_bps"]) for r in rec if r.get("max_realized_slippage_bps") is not None]
    fees_vals = [float(r["fees_total"] or 0.0) for r in rec]
    net_vals = [float(r["net_pnl_usd"] or 0.0) for r in rec]
    projected_edge_vals = [float(r['projected_move_bps']) - float(r['projected_cost_bps']) for r in rec if r.get('projected_move_bps') is not None and r.get('projected_cost_bps') is not None]
    realized_edge_vals = [float(r['net_edge_bps']) for r in rec if r.get('net_edge_bps') is not None]
    blocked = _blocked_trade_summary(days)
    maker_entry = sum(1 for r in rec if str(r.get('entry_liquidity_role') or '') == 'maker')
    taker_entry = sum(1 for r in rec if str(r.get('entry_liquidity_role') or '') == 'taker')
    maker_exit = sum(1 for r in rec if str(r.get('exit_liquidity_role') or '') == 'maker')
    taker_exit = sum(1 for r in rec if str(r.get('exit_liquidity_role') or '') == 'taker')
    return {
        "ok": True,
        "db_path": _db_path(),
        "days": float(days),
        "closed_trades": total,
        "clean_closed_trades": clean,
        "unclean_closed_trades": total - clean,
        "net_pnl_usd": sum(net_vals),
        "fees_total_usd": sum(fees_vals),
        "avg_max_realized_slippage_bps": (sum(slippage_vals) / len(slippage_vals)) if slippage_vals else None,
        "max_realized_slippage_bps": max(slippage_vals) if slippage_vals else None,
        "avg_projected_edge_bps": (sum(projected_edge_vals) / len(projected_edge_vals)) if projected_edge_vals else None,
        "avg_realized_edge_bps": (sum(realized_edge_vals) / len(realized_edge_vals)) if realized_edge_vals else None,
        "maker_vs_taker": {
            'entry': {'maker': maker_entry, 'taker': taker_entry, 'other': max(0, total - maker_entry - taker_entry)},
            'exit': {'maker': maker_exit, 'taker': taker_exit, 'other': max(0, total - maker_exit - taker_exit)},
        },
        "execution_truth": _execution_truth_summary(rec),
        "strategy_truth": _strategy_truth_summary(rec),
        "expectancy_by_hold_bucket": _group_stats(rec, 'hold_bucket'),
        "expectancy_by_regime": _group_stats(rec, 'regime_state'),
        "blocked_trade_summary": blocked,
        "latest_closed_ts": rec[0].get("closed_ts") if rec else None,
        "open_trades": len(trade_journal.list_open_trades(limit=5000)),
    }


def live_validation_summary() -> Dict[str, Any]:
    s = summary(days=30.0)
    eth_after = _env_int("PROMOTION_ENABLE_ETH_AFTER_CLEAN_TRADES", 10)
    size_after = _env_int("PROMOTION_ENABLE_SIZE_UP_AFTER_CLEAN_TRADES", 30)
    recent_limit = _env_int("TELEMETRY_MAX_RECENT_TRADES", 100)
    recent = recent_trades(limit=recent_limit)
    clean = int(s.get("clean_closed_trades") or 0)
    return {
        "ok": True,
        "mode_enabled": os.getenv("LIVE_VALIDATION_MODE", "1").strip().lower() in ("1", "true", "yes", "on"),
        "require_scanner_ok": os.getenv("LIVE_VALIDATION_REQUIRE_SCANNER_OK", "0").strip().lower() in ("1", "true", "yes", "on"),
        "promotion_thresholds": {
            "enable_eth_after_clean_trades": eth_after,
            "enable_size_up_after_clean_trades": size_after,
        },
        "promotion_status": {
            "clean_closed_trades": clean,
            "eth_gate_passed": clean >= eth_after,
            "size_up_gate_passed": clean >= size_after,
        },
        "telemetry": s,
        "recent_trade_count": len(recent),
        "recent_alert_trade_count": sum(1 for r in recent if str(r.get("alert_flags_json") or "[]") not in ("[]", "", "null")),
    }
