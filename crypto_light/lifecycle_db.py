from __future__ import annotations

import json
import os
import sqlite3
import time
from typing import Any, Dict, List, Optional

DEFAULT_DB_PATH = "/var/data/lifecycle.sqlite3"

OPENISH_TRADE_PLAN_STATUSES = {'approved', 'submitted', 'active'}
TERMINAL_TRADE_PLAN_STATUSES = {'closed', 'cancelled', 'rejected', 'failed', 'failed_reconcile', 'expired', 'abandoned'}
OPENISH_ORDER_INTENT_STATES = {'created','validated','submitted','acknowledged','partial','replace_pending','cancel_pending'}
ADMISSION_RESULT_FINAL = {'accepted', 'rejected', 'duplicate'}

TERMINAL_REASON_ALIASES = {
    'duplicate_signal_id': 'rejected_duplicate_signal',
    'duplicate_signal_fingerprint': 'rejected_duplicate_signal',
    'trading_disabled': 'rejected_trading_disabled',
    'max_entries_per_day_reached': 'rejected_max_entries_per_day',
    'global_entry_cooldown': 'rejected_global_entry_cooldown',
    'max_daily_loss_reached': 'rejected_max_daily_loss',
    'entry_cooldown': 'rejected_entry_cooldown',
    'entry_failure_cooldown': 'rejected_cooldown_active',
    'max_trades_per_symbol_per_day': 'rejected_max_trades_per_symbol_per_day',
    'position_already_open': 'rejected_position_exists',
    'open_position_exists': 'rejected_position_exists',
    'entry_order_lock_active': 'rejected_order_lock_active',
    'broker_open_buy_order_exists': 'rejected_broker_open_order_exists',
    'active_workflow_lock': 'rejected_symbol_locked',
    'open_trade_plan_exists': 'rejected_open_trade_plan_exists',
    'open_order_intent_exists': 'rejected_open_intent_exists',
    'broker_balance_unavailable': 'rejected_broker_balance_unavailable',
    'ops_risk_lockout_active': 'rejected_ops_risk_lockout',
    'spread_too_wide': 'rejected_spread_too_wide',
    'scanner_symbol_not_allowed': 'rejected_scanner_symbol_not_allowed',
    'signal_rejected_by_scanner': 'rejected_scanner_symbol_not_allowed',
    'min_cash_buffer_breach': 'rejected_min_cash_buffer_breach',
    'insufficient_cash_usd_estimate': 'rejected_insufficient_cash',
    'risk_admission_reject': 'rejected_risk_admission',
    'pretrade_health_gate_closed': 'rejected_pretrade_health_gate',
    'submit_failed_pre_ack': 'submit_failed_pre_ack',
    'broker_rejected': 'broker_rejected',
    'intent_timeout': 'intent_timeout',
    'reconcile_cleanup': 'reconcile_cleanup',
    'reconcile_missing_broker_order': 'reconcile_cleanup',
    'exit_order_failed': 'broker_rejected',
    'insufficient funds': 'broker_rejected_insufficient_funds',
    'insufficient_funds': 'broker_rejected_insufficient_funds',
    'not enough': 'broker_rejected_insufficient_funds',
    'post only': 'broker_rejected_post_only',
    'post-only': 'broker_rejected_post_only',
    'would take liquidity': 'broker_rejected_post_only',
    'rate limit': 'broker_rejected_rate_limited',
    'throttle': 'broker_rejected_rate_limited',
    'timeout': 'intent_timeout',
    'timed out': 'intent_timeout',
    'cancelled': 'broker_cancelled',
    'canceled': 'broker_cancelled',
    'stop_loss': 'exit_stop_loss',
    'take_profit': 'exit_take_profit',
    'time_stop': 'exit_time_stop',
}


def normalize_terminal_reason(reason: str | None, *, default: str | None = None) -> str | None:
    raw = str(reason or '').strip()
    if not raw:
        return default
    if raw.startswith('rejected_') or raw.startswith('exit_') or raw.startswith('broker_') or raw in {'submit_failed_pre_ack', 'intent_timeout', 'reconcile_cleanup'}:
        return raw
    lowered = raw.lower()
    if raw in TERMINAL_REASON_ALIASES:
        return TERMINAL_REASON_ALIASES[raw]
    if lowered in TERMINAL_REASON_ALIASES:
        return TERMINAL_REASON_ALIASES[lowered]
    for needle, normalized in TERMINAL_REASON_ALIASES.items():
        if needle in lowered:
            return normalized
    return raw


def _nonempty(v: Any) -> Any:
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def _db_path() -> str:
    return (os.getenv("LIFECYCLE_DB_PATH") or DEFAULT_DB_PATH).strip() or DEFAULT_DB_PATH


def _connect() -> sqlite3.Connection:
    path = _db_path()
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    con = sqlite3.connect(path)
    con.row_factory = sqlite3.Row
    return con


def _json(x: Any) -> str:
    try:
        return json.dumps(x if x is not None else {}, separators=(",", ":"), sort_keys=True)
    except Exception:
        return "{}"


def _sqlite_scalar(x: Any) -> Any:
    if isinstance(x, (dict, list, tuple)):
        return _json(x)
    return x


def _sanitize_payload(payload: Dict[str, Any], *, json_fields: Optional[set[str]] = None) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    jf = set(json_fields or set())
    for k, v in dict(payload or {}).items():
        if k in jf and not isinstance(v, str):
            out[k] = _json(v)
        else:
            out[k] = _sqlite_scalar(v)
    return out


def _ensure_columns(con: sqlite3.Connection, table: str, cols: Dict[str, str]) -> None:
    existing = {r['name'] for r in con.execute(f"PRAGMA table_info({table})").fetchall()}
    for name, ddl in cols.items():
        if name not in existing:
            con.execute(f"ALTER TABLE {table} ADD COLUMN {name} {ddl}")


def ensure_schema() -> str:
    con = _connect()
    try:
        con.executescript(
            """
            CREATE TABLE IF NOT EXISTS trade_plans (
                trade_plan_id TEXT PRIMARY KEY,
                symbol TEXT NOT NULL,
                strategy_id TEXT,
                signal_id TEXT,
                status TEXT NOT NULL,
                direction TEXT,
                entry_mode TEXT,
                entry_ref_price REAL,
                stop_price REAL,
                target_price REAL,
                time_stop_sec INTEGER,
                requested_notional_usd REAL,
                approved_notional_usd REAL,
                risk_snapshot_json TEXT,
                legacy_symbol_key TEXT,
                created_ts REAL,
                updated_ts REAL,
                expires_ts REAL,
                closed_ts REAL
            );
            CREATE INDEX IF NOT EXISTS idx_trade_plans_legacy_symbol_key ON trade_plans(legacy_symbol_key);
            CREATE INDEX IF NOT EXISTS idx_trade_plans_symbol_status ON trade_plans(symbol, status);
            CREATE INDEX IF NOT EXISTS idx_trade_plans_created_ts ON trade_plans(created_ts);

            CREATE TABLE IF NOT EXISTS order_intents (
                intent_id TEXT PRIMARY KEY,
                trade_plan_id TEXT,
                symbol TEXT NOT NULL,
                side TEXT,
                order_type TEXT,
                strategy_id TEXT,
                state TEXT NOT NULL,
                desired_qty REAL,
                desired_notional_usd REAL,
                limit_price REAL,
                broker_txid TEXT,
                filled_qty REAL,
                avg_fill_price REAL,
                fees_usd REAL,
                retry_count INTEGER,
                reject_reason TEXT,
                cancel_reason TEXT,
                client_order_key TEXT,
                last_broker_status TEXT,
                remaining_qty REAL,
                submitted_ts REAL,
                acknowledged_ts REAL,
                raw_json TEXT,
                created_ts REAL,
                updated_ts REAL
            );
            CREATE INDEX IF NOT EXISTS idx_order_intents_plan_state ON order_intents(trade_plan_id, state);
            CREATE INDEX IF NOT EXISTS idx_order_intents_symbol_created ON order_intents(symbol, created_ts);

            CREATE TABLE IF NOT EXISTS broker_orders (
                broker_order_id TEXT PRIMARY KEY,
                intent_id TEXT,
                trade_plan_id TEXT,
                symbol TEXT NOT NULL,
                strategy_id TEXT,
                side TEXT,
                order_type TEXT,
                lifecycle_stage TEXT,
                status TEXT NOT NULL,
                client_order_key TEXT,
                broker_txid TEXT,
                requested_qty REAL,
                requested_notional_usd REAL,
                limit_price REAL,
                avg_fill_price REAL,
                filled_qty REAL,
                remaining_qty REAL,
                fees_usd REAL,
                reject_reason TEXT,
                raw_json TEXT,
                created_ts REAL,
                updated_ts REAL,
                acknowledged_ts REAL,
                closed_ts REAL
            );
            CREATE INDEX IF NOT EXISTS idx_broker_orders_symbol_status ON broker_orders(symbol, status);
            CREATE INDEX IF NOT EXISTS idx_broker_orders_intent_created ON broker_orders(intent_id, created_ts);
            CREATE INDEX IF NOT EXISTS idx_broker_orders_plan_created ON broker_orders(trade_plan_id, created_ts);

            CREATE TABLE IF NOT EXISTS trade_lifecycle_events (
                event_id TEXT PRIMARY KEY,
                trade_plan_id TEXT,
                intent_id TEXT,
                broker_order_id TEXT,
                position_id TEXT,
                symbol TEXT,
                strategy_id TEXT,
                stage TEXT NOT NULL,
                event_type TEXT NOT NULL,
                reason TEXT,
                payload_json TEXT,
                created_ts REAL
            );
            CREATE INDEX IF NOT EXISTS idx_trade_lifecycle_plan_created ON trade_lifecycle_events(trade_plan_id, created_ts);
            CREATE INDEX IF NOT EXISTS idx_trade_lifecycle_symbol_created ON trade_lifecycle_events(symbol, created_ts);
            CREATE INDEX IF NOT EXISTS idx_trade_lifecycle_stage_created ON trade_lifecycle_events(stage, created_ts);

            CREATE TABLE IF NOT EXISTS position_ledger (
                position_id TEXT PRIMARY KEY,
                trade_plan_id TEXT,
                symbol TEXT NOT NULL,
                side TEXT,
                qty REAL,
                avg_entry_price REAL,
                notional_usd REAL,
                realized_pnl_usd REAL,
                unrealized_pnl_usd REAL,
                fees_usd REAL,
                status TEXT NOT NULL,
                broker_position_qty REAL,
                opened_ts REAL,
                updated_ts REAL,
                closed_ts REAL,
                raw_json TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_position_ledger_symbol_status ON position_ledger(symbol, status);
            CREATE INDEX IF NOT EXISTS idx_position_ledger_trade_plan_id ON position_ledger(trade_plan_id);

            CREATE TABLE IF NOT EXISTS fill_events (
                fill_id TEXT PRIMARY KEY,
                intent_id TEXT,
                trade_plan_id TEXT,
                symbol TEXT,
                side TEXT,
                price REAL,
                qty REAL,
                notional_usd REAL,
                fee_usd REAL,
                fill_ts REAL,
                broker_txid TEXT,
                raw_json TEXT,
                created_ts REAL
            );
            CREATE INDEX IF NOT EXISTS idx_fill_events_intent_id ON fill_events(intent_id);
            CREATE INDEX IF NOT EXISTS idx_fill_events_symbol_fill_ts ON fill_events(symbol, fill_ts);

            CREATE TABLE IF NOT EXISTS anomalies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                kind TEXT NOT NULL,
                severity TEXT NOT NULL,
                symbol TEXT,
                trade_plan_id TEXT,
                intent_id TEXT,
                details_json TEXT,
                created_ts REAL,
                resolved_ts REAL
            );

            CREATE INDEX IF NOT EXISTS idx_anomalies_open ON anomalies(resolved_ts, severity, created_ts);

            CREATE TABLE IF NOT EXISTS ops_events (
                event_id TEXT PRIMARY KEY,
                event_type TEXT NOT NULL,
                symbol TEXT,
                strategy_id TEXT,
                signal_id TEXT,
                fingerprint TEXT,
                reason TEXT,
                payload_json TEXT,
                created_ts REAL
            );
            CREATE INDEX IF NOT EXISTS idx_ops_events_symbol_created ON ops_events(symbol, created_ts);
            CREATE INDEX IF NOT EXISTS idx_ops_events_type_created ON ops_events(event_type, created_ts);

            CREATE TABLE IF NOT EXISTS workflow_locks (
                lock_key TEXT PRIMARY KEY,
                symbol TEXT NOT NULL,
                strategy_id TEXT,
                stage TEXT,
                owner_req_id TEXT,
                created_ts REAL,
                expires_ts REAL,
                released_ts REAL,
                metadata_json TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_workflow_locks_symbol_active ON workflow_locks(symbol, released_ts, expires_ts);

            CREATE TABLE IF NOT EXISTS signal_fingerprints (
                fingerprint TEXT PRIMARY KEY,
                symbol TEXT,
                strategy_id TEXT,
                signal_id TEXT,
                first_seen_ts REAL,
                last_seen_ts REAL,
                expires_ts REAL,
                metadata_json TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_signal_fingerprints_expires ON signal_fingerprints(expires_ts);

            CREATE TABLE IF NOT EXISTS admission_events (
                event_id TEXT PRIMARY KEY,
                symbol TEXT NOT NULL,
                strategy_id TEXT,
                signal_id TEXT,
                signal_name TEXT,
                fingerprint TEXT,
                source TEXT,
                bar_ts INTEGER,
                trigger_price REAL,
                reference_price REAL,
                atr REAL,
                range_high REAL,
                range_low REAL,
                range_width_pct REAL,
                breakout_level REAL,
                breakout_distance_pct REAL,
                ranking_score REAL,
                spread_pct REAL,
                regime_state TEXT,
                admission_result TEXT,
                reject_reason TEXT,
                payload_json TEXT,
                created_ts REAL
            );
            CREATE INDEX IF NOT EXISTS idx_admission_events_symbol_created ON admission_events(symbol, created_ts);
            CREATE INDEX IF NOT EXISTS idx_admission_events_result_created ON admission_events(admission_result, created_ts);
            """
        )
        _ensure_columns(con, 'trade_plans', {
            'signal_id': 'TEXT',
            'entry_mode': 'TEXT',
            'legacy_symbol_key': 'TEXT',
            'risk_snapshot_json': 'TEXT',
            'expires_ts': 'REAL',
            'closed_ts': 'REAL',
        })
        con.execute('DROP INDEX IF EXISTS idx_trade_plans_legacy_symbol_key')
        con.execute('CREATE INDEX IF NOT EXISTS idx_trade_plans_legacy_symbol_key ON trade_plans(legacy_symbol_key)')
        _ensure_columns(con, 'order_intents', {
            'cancel_reason': 'TEXT',
            'client_order_key': 'TEXT',
            'last_broker_status': 'TEXT',
            'remaining_qty': 'REAL',
            'submitted_ts': 'REAL',
            'acknowledged_ts': 'REAL',
        })
        _ensure_columns(con, 'ops_events', {
            'event_id': 'TEXT',
            'event_type': 'TEXT',
            'symbol': 'TEXT',
            'strategy_id': 'TEXT',
            'signal_id': 'TEXT',
            'fingerprint': 'TEXT',
            'reason': 'TEXT',
            'payload_json': 'TEXT',
            'created_ts': 'REAL',
        })
        _ensure_columns(con, 'workflow_locks', {
            'lock_key': 'TEXT',
            'symbol': 'TEXT',
            'strategy_id': 'TEXT',
            'stage': 'TEXT',
            'owner_req_id': 'TEXT',
            'created_ts': 'REAL',
            'expires_ts': 'REAL',
            'released_ts': 'REAL',
            'metadata_json': 'TEXT',
        })
        _ensure_columns(con, 'signal_fingerprints', {
            'fingerprint': 'TEXT',
            'symbol': 'TEXT',
            'strategy_id': 'TEXT',
            'signal_id': 'TEXT',
            'first_seen_ts': 'REAL',
            'last_seen_ts': 'REAL',
            'expires_ts': 'REAL',
            'metadata_json': 'TEXT',
        })
        _ensure_columns(con, 'admission_events', {
            'event_id': 'TEXT',
            'symbol': 'TEXT',
            'strategy_id': 'TEXT',
            'signal_id': 'TEXT',
            'signal_name': 'TEXT',
            'fingerprint': 'TEXT',
            'source': 'TEXT',
            'bar_ts': 'INTEGER',
            'trigger_price': 'REAL',
            'reference_price': 'REAL',
            'atr': 'REAL',
            'range_high': 'REAL',
            'range_low': 'REAL',
            'range_width_pct': 'REAL',
            'breakout_level': 'REAL',
            'breakout_distance_pct': 'REAL',
            'ranking_score': 'REAL',
            'spread_pct': 'REAL',
            'regime_state': 'TEXT',
            'admission_result': 'TEXT',
            'reject_reason': 'TEXT',
            'payload_json': 'TEXT',
            'created_ts': 'REAL',
        })
        con.commit()
    finally:
        con.close()
    return _db_path()


def _prepare_trade_plan_payload(plan: Dict[str, Any]) -> Dict[str, Any]:
    now = time.time()
    payload = dict(plan or {})
    payload.setdefault('status', 'created')
    payload.setdefault('created_ts', now)
    payload['updated_ts'] = now
    payload['reject_reason'] = normalize_terminal_reason(payload.get('reject_reason'))
    payload.setdefault('risk_snapshot_json', _json(payload.get('risk_snapshot_json') or payload.get('risk_snapshot') or {}))

    created_ts = payload.get('created_ts', now)
    try:
        created_ts_f = float(created_ts)
    except Exception:
        created_ts_f = now
        payload['created_ts'] = now

    time_stop_raw = payload.get('time_stop_sec', 0)
    try:
        time_stop_sec = int(float(time_stop_raw or 0))
    except Exception:
        time_stop_sec = 0

    default_expiry = created_ts_f + float(time_stop_sec if time_stop_sec > 0 else 3600)
    payload.setdefault('expires_ts', default_expiry)
    payload.setdefault('closed_ts', None)
    payload.setdefault('legacy_symbol_key', payload.get('symbol'))
    payload['risk_snapshot_json'] = _json(payload.get('risk_snapshot_json') or payload.get('risk_snapshot') or {})
    for numeric_key in (
        'entry_ref_price', 'stop_price', 'target_price', 'requested_notional_usd',
        'approved_notional_usd', 'created_ts', 'expires_ts', 'closed_ts'
    ):
        if numeric_key in payload and payload.get(numeric_key) is not None:
            try:
                payload[numeric_key] = float(payload.get(numeric_key))
            except Exception:
                if numeric_key in ('created_ts', 'expires_ts'):
                    payload[numeric_key] = float(default_expiry if numeric_key == 'expires_ts' else created_ts_f)
                else:
                    payload[numeric_key] = None
    try:
        payload['time_stop_sec'] = int(float(payload.get('time_stop_sec') or 0))
    except Exception:
        payload['time_stop_sec'] = 0
    payload = _sanitize_payload(payload, json_fields={'risk_snapshot_json'})
    payload['risk_snapshot_json'] = _json(payload.get('risk_snapshot_json') if isinstance(payload.get('risk_snapshot_json'), (dict, list, tuple)) else payload.get('risk_snapshot_json') or {}) if not isinstance(payload.get('risk_snapshot_json'), str) else payload.get('risk_snapshot_json')
    return payload


def _execute_trade_plan_upsert(con: sqlite3.Connection, payload: Dict[str, Any]) -> None:
    con.execute(
        """
        INSERT INTO trade_plans (
            trade_plan_id, symbol, strategy_id, signal_id, status, direction, entry_mode,
            entry_ref_price, stop_price, target_price, time_stop_sec,
            requested_notional_usd, approved_notional_usd, risk_snapshot_json,
            legacy_symbol_key, created_ts, updated_ts, expires_ts, closed_ts
        ) VALUES (
            :trade_plan_id, :symbol, :strategy_id, :signal_id, :status, :direction, :entry_mode,
            :entry_ref_price, :stop_price, :target_price, :time_stop_sec,
            :requested_notional_usd, :approved_notional_usd, :risk_snapshot_json,
            :legacy_symbol_key, :created_ts, :updated_ts, :expires_ts, :closed_ts
        )
        ON CONFLICT(trade_plan_id) DO UPDATE SET
            symbol=excluded.symbol,
            strategy_id=excluded.strategy_id,
            signal_id=excluded.signal_id,
            status=excluded.status,
            direction=excluded.direction,
            entry_mode=excluded.entry_mode,
            entry_ref_price=excluded.entry_ref_price,
            stop_price=excluded.stop_price,
            target_price=excluded.target_price,
            time_stop_sec=excluded.time_stop_sec,
            requested_notional_usd=excluded.requested_notional_usd,
            approved_notional_usd=excluded.approved_notional_usd,
            risk_snapshot_json=excluded.risk_snapshot_json,
            legacy_symbol_key=excluded.legacy_symbol_key,
            updated_ts=excluded.updated_ts,
            expires_ts=excluded.expires_ts,
            closed_ts=excluded.closed_ts
        """,
        payload,
    )


_ORDER_INTENT_SQL_KEYS = (
    'intent_id', 'trade_plan_id', 'symbol', 'side', 'order_type', 'strategy_id', 'state',
    'desired_qty', 'desired_notional_usd', 'limit_price', 'broker_txid',
    'filled_qty', 'avg_fill_price', 'fees_usd', 'retry_count', 'reject_reason',
    'cancel_reason', 'client_order_key', 'last_broker_status', 'remaining_qty',
    'submitted_ts', 'acknowledged_ts', 'raw_json', 'created_ts', 'updated_ts',
)


def _prepare_order_intent_payload(intent: Dict[str, Any]) -> Dict[str, Any]:
    now = time.time()
    payload = dict(intent or {})
    payload.setdefault('state', 'created')
    payload.setdefault('created_ts', now)
    payload['updated_ts'] = now
    payload.setdefault('retry_count', 0)
    payload.setdefault('remaining_qty', payload.get('desired_qty'))
    payload.setdefault('broker_txid', None)
    payload.setdefault('filled_qty', None)
    payload.setdefault('avg_fill_price', None)
    payload.setdefault('fees_usd', None)
    payload.setdefault('reject_reason', None)
    payload.setdefault('cancel_reason', None)
    payload.setdefault('client_order_key', None)
    payload.setdefault('last_broker_status', None)
    payload.setdefault('submitted_ts', now if str(payload.get('state') or '') in {'submitted','acknowledged','filled','partial','cancel_pending','cancelled','failed_reconcile'} else None)
    payload.setdefault('acknowledged_ts', now if str(payload.get('state') or '') in {'acknowledged','filled','partial','cancel_pending','cancelled','failed_reconcile'} else None)
    payload.setdefault('raw_json', _json(payload.get('raw_json') or payload))
    payload = _sanitize_payload(payload, json_fields={'raw_json'})
    for key in _ORDER_INTENT_SQL_KEYS:
        payload.setdefault(key, None)
    payload['reject_reason'] = normalize_terminal_reason(payload.get('reject_reason'))
    payload['cancel_reason'] = normalize_terminal_reason(payload.get('cancel_reason'))
    return payload


def _execute_order_intent_upsert(con: sqlite3.Connection, payload: Dict[str, Any]) -> None:
    """Persist an order intent using positional binds only.

    We intentionally avoid named-parameter execution here because repeated regressions
    have shown that one live call path can still arrive with optional keys omitted,
    most notably ``broker_txid``. Positional binding removes that entire failure class
    by building a fixed-length tuple from the canonical SQL key list.
    """
    sanitized = _sanitize_payload(dict(payload or {}), json_fields={'raw_json'})
    values_by_key = {key: sanitized.get(key, None) for key in _ORDER_INTENT_SQL_KEYS}
    missing_keys = [key for key in _ORDER_INTENT_SQL_KEYS if key not in values_by_key]
    if missing_keys:
        raise RuntimeError(f"order_intent positional payload missing keys: {missing_keys}")
    values = tuple(values_by_key[key] for key in _ORDER_INTENT_SQL_KEYS)
    if len(values) != len(_ORDER_INTENT_SQL_KEYS):
        raise RuntimeError(
            f"order_intent positional payload size mismatch: expected {len(_ORDER_INTENT_SQL_KEYS)} got {len(values)}"
        )
    con.execute(
        """
        INSERT INTO order_intents (
            intent_id, trade_plan_id, symbol, side, order_type, strategy_id, state,
            desired_qty, desired_notional_usd, limit_price, broker_txid,
            filled_qty, avg_fill_price, fees_usd, retry_count, reject_reason,
            cancel_reason, client_order_key, last_broker_status, remaining_qty,
            submitted_ts, acknowledged_ts, raw_json, created_ts, updated_ts
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?,
            ?, ?, ?, ?, ?,
            ?, ?, ?, ?,
            ?, ?, ?, ?, ?
        )
        ON CONFLICT(intent_id) DO UPDATE SET
            trade_plan_id=excluded.trade_plan_id,
            symbol=excluded.symbol,
            side=excluded.side,
            order_type=excluded.order_type,
            strategy_id=excluded.strategy_id,
            state=excluded.state,
            desired_qty=excluded.desired_qty,
            desired_notional_usd=excluded.desired_notional_usd,
            limit_price=excluded.limit_price,
            broker_txid=excluded.broker_txid,
            filled_qty=excluded.filled_qty,
            avg_fill_price=excluded.avg_fill_price,
            fees_usd=excluded.fees_usd,
            retry_count=excluded.retry_count,
            reject_reason=excluded.reject_reason,
            cancel_reason=excluded.cancel_reason,
            client_order_key=excluded.client_order_key,
            last_broker_status=excluded.last_broker_status,
            remaining_qty=excluded.remaining_qty,
            submitted_ts=excluded.submitted_ts,
            acknowledged_ts=excluded.acknowledged_ts,
            raw_json=excluded.raw_json,
            updated_ts=excluded.updated_ts
        """,
        values,
    )


def create_entry_records_atomic(plan: Dict[str, Any], intent: Dict[str, Any]) -> None:
    ensure_schema()
    plan_payload = _prepare_trade_plan_payload(plan)
    intent_payload = _prepare_order_intent_payload(intent)
    con = _connect()
    try:
        _execute_trade_plan_upsert(con, plan_payload)
        _execute_order_intent_upsert(con, intent_payload)
        con.commit()
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()


def upsert_trade_plan(plan: Dict[str, Any]) -> None:
    ensure_schema()
    payload = _prepare_trade_plan_payload(plan)
    con = _connect()
    try:
        _execute_trade_plan_upsert(con, payload)
        con.commit()
    finally:
        con.close()


def update_trade_plan_status(trade_plan_id: str, status: str, **fields: Any) -> None:
    ensure_schema()
    allowed = {
        'signal_id', 'direction', 'entry_mode', 'entry_ref_price', 'stop_price', 'target_price',
        'time_stop_sec', 'requested_notional_usd', 'approved_notional_usd', 'risk_snapshot_json',
        'legacy_symbol_key', 'expires_ts', 'closed_ts'
    }
    sets = ['status = ?', 'updated_ts = ?']
    args: List[Any] = [status, time.time()]
    for k, v in fields.items():
        if k in allowed:
            sets.append(f"{k} = ?")
            if k == 'risk_snapshot_json' and not isinstance(v, str):
                v = _json(v)
            elif isinstance(v, (dict, list, tuple)):
                v = _json(v)
            args.append(v)
    args.append(trade_plan_id)
    con = _connect()
    try:
        con.execute(f"UPDATE trade_plans SET {', '.join(sets)} WHERE trade_plan_id = ?", args)
        con.commit()
    finally:
        con.close()


def upsert_order_intent(intent: Dict[str, Any]) -> None:
    ensure_schema()
    payload = _prepare_order_intent_payload(intent)
    con = _connect()
    try:
        _execute_order_intent_upsert(con, payload)
        con.commit()
    finally:
        con.close()


def update_order_intent(intent_id: str, **fields: Any) -> None:
    ensure_schema()
    if not fields:
        return
    allowed = {
        'trade_plan_id', 'symbol', 'side', 'order_type', 'strategy_id', 'state', 'desired_qty',
        'desired_notional_usd', 'limit_price', 'broker_txid', 'filled_qty', 'avg_fill_price',
        'fees_usd', 'retry_count', 'reject_reason', 'cancel_reason', 'client_order_key',
        'last_broker_status', 'remaining_qty', 'submitted_ts', 'acknowledged_ts', 'raw_json'
    }
    sets = ['updated_ts = ?']
    args: List[Any] = [time.time()]
    for k, v in fields.items():
        if k in allowed:
            sets.append(f"{k} = ?")
            if k in {'reject_reason','cancel_reason'}:
                v = normalize_terminal_reason(v)
            if k == 'raw_json' and not isinstance(v, str):
                v = _json(v)
            args.append(v)
    if len(sets) <= 1:
        return
    args.append(intent_id)
    con = _connect()
    try:
        con.execute(f"UPDATE order_intents SET {', '.join(sets)} WHERE intent_id = ?", args)
        con.commit()
    finally:
        con.close()


def get_order_intent(intent_id: str) -> Optional[Dict[str, Any]]:
    ensure_schema()
    con = _connect()
    try:
        row = con.execute("SELECT * FROM order_intents WHERE intent_id = ?", (intent_id,)).fetchone()
        return dict(row) if row else None
    finally:
        con.close()


def transition_order_intent(intent_id: str, new_state: str, **fields: Any) -> None:
    now = time.time()
    payload = dict(fields or {})
    payload['state'] = str(new_state or '')
    state_l = str(new_state or '').strip().lower()
    if state_l in {'submitted','acknowledged','filled','partial','cancel_pending','cancelled','failed_reconcile'} and 'submitted_ts' not in payload:
        payload['submitted_ts'] = now
    if state_l in {'acknowledged','filled','partial','cancel_pending','cancelled','failed_reconcile'} and 'acknowledged_ts' not in payload:
        payload['acknowledged_ts'] = now
    update_order_intent(intent_id, **payload)


def execution_state_summary(limit: int = 200) -> Dict[str, Any]:
    rows = list_rows('order_intents', limit=limit, order_by='updated_ts DESC')
    counts: Dict[str, int] = {}
    for row in rows:
        st = str(row.get('state') or 'unknown')
        counts[st] = counts.get(st, 0) + 1
    stale_unfinished = 0
    now = time.time()
    for row in rows:
        st = str(row.get('state') or '')
        if st in {'submitted','acknowledged','partial','cancel_pending','replace_pending','failed_reconcile'}:
            upd = float(row.get('updated_ts') or 0.0)
            if upd > 0 and (now - upd) > 120:
                stale_unfinished += 1
    return {
        'ok': True,
        'db_path': _db_path(),
        'recent_intents': len(rows),
        'state_counts': counts,
        'stale_unfinished_recent': int(stale_unfinished),
    }


def upsert_position_ledger(position: Dict[str, Any]) -> None:
    ensure_schema()
    now = time.time()
    payload = dict(position or {})
    payload.setdefault('status', 'open')
    payload.setdefault('opened_ts', now)
    payload['updated_ts'] = now
    payload.setdefault('raw_json', _json(payload.get('raw_json') or payload))
    payload = _sanitize_payload(payload, json_fields={'raw_json'})
    con = _connect()
    try:
        con.execute(
            """
            INSERT INTO position_ledger (
                position_id, trade_plan_id, symbol, side, qty, avg_entry_price, notional_usd,
                realized_pnl_usd, unrealized_pnl_usd, fees_usd, status,
                broker_position_qty, opened_ts, updated_ts, closed_ts, raw_json
            ) VALUES (
                :position_id, :trade_plan_id, :symbol, :side, :qty, :avg_entry_price, :notional_usd,
                :realized_pnl_usd, :unrealized_pnl_usd, :fees_usd, :status,
                :broker_position_qty, :opened_ts, :updated_ts, :closed_ts, :raw_json
            )
            ON CONFLICT(position_id) DO UPDATE SET
                trade_plan_id=excluded.trade_plan_id,
                symbol=excluded.symbol,
                side=excluded.side,
                qty=excluded.qty,
                avg_entry_price=excluded.avg_entry_price,
                notional_usd=excluded.notional_usd,
                realized_pnl_usd=excluded.realized_pnl_usd,
                unrealized_pnl_usd=excluded.unrealized_pnl_usd,
                fees_usd=excluded.fees_usd,
                status=excluded.status,
                broker_position_qty=excluded.broker_position_qty,
                updated_ts=excluded.updated_ts,
                closed_ts=excluded.closed_ts,
                raw_json=excluded.raw_json
            """,
            payload,
        )
        con.commit()
    finally:
        con.close()


def insert_fill_event(fill: Dict[str, Any]) -> None:
    ensure_schema()
    payload = dict(fill or {})
    payload.setdefault('created_ts', time.time())
    payload.setdefault('raw_json', _json(payload.get('raw_json') or fill))
    payload = _sanitize_payload(payload, json_fields={'raw_json'})
    con = _connect()
    try:
        con.execute(
            """
            INSERT OR REPLACE INTO fill_events (
                fill_id, intent_id, trade_plan_id, symbol, side, price, qty,
                notional_usd, fee_usd, fill_ts, broker_txid, raw_json, created_ts
            ) VALUES (
                :fill_id, :intent_id, :trade_plan_id, :symbol, :side, :price, :qty,
                :notional_usd, :fee_usd, :fill_ts, :broker_txid, :raw_json, :created_ts
            )
            """,
            payload,
        )
        con.commit()
    finally:
        con.close()


def record_anomaly(kind: str, severity: str = 'warn', *, symbol: str | None = None,
                   trade_plan_id: str | None = None, intent_id: str | None = None,
                   details: Optional[Dict[str, Any]] = None) -> int:
    ensure_schema()
    con = _connect()
    try:
        if trade_plan_id:
            row = con.execute(
                'SELECT id FROM anomalies WHERE kind = ? AND trade_plan_id = ? AND resolved_ts IS NULL ORDER BY id DESC LIMIT 1',
                (kind, trade_plan_id),
            ).fetchone()
            if row:
                return int(row['id'] or 0)
        if intent_id:
            row = con.execute(
                'SELECT id FROM anomalies WHERE kind = ? AND intent_id = ? AND resolved_ts IS NULL ORDER BY id DESC LIMIT 1',
                (kind, intent_id),
            ).fetchone()
            if row:
                return int(row['id'] or 0)
        cur = con.execute(
            """
            INSERT INTO anomalies(kind, severity, symbol, trade_plan_id, intent_id, details_json, created_ts, resolved_ts)
            VALUES (?, ?, ?, ?, ?, ?, ?, NULL)
            """,
            (kind, severity, symbol, trade_plan_id, intent_id, _json(details or {}), time.time()),
        )
        con.commit()
        return int(cur.lastrowid or 0)
    finally:
        con.close()


def get_trade_plan(trade_plan_id: str) -> Optional[Dict[str, Any]]:
    rows = list_rows('trade_plans', limit=1, where='trade_plan_id = ?', args=[trade_plan_id], order_by='updated_ts DESC')
    return rows[0] if rows else None


def resolve_anomalies(*, kind: str | None = None, trade_plan_id: str | None = None,
                     intent_id: str | None = None, symbol: str | None = None) -> int:
    ensure_schema()
    clauses = ['resolved_ts IS NULL']
    args: List[Any] = []
    if kind is not None:
        clauses.append('kind = ?')
        args.append(kind)
    if trade_plan_id is not None:
        clauses.append('trade_plan_id = ?')
        args.append(trade_plan_id)
    if intent_id is not None:
        clauses.append('intent_id = ?')
        args.append(intent_id)
    if symbol is not None:
        clauses.append('symbol = ?')
        args.append(symbol)
    where = ' AND '.join(clauses)
    con = _connect()
    try:
        cur = con.execute(f'UPDATE anomalies SET resolved_ts = ? WHERE {where}', [time.time(), *args])
        con.commit()
        return int(cur.rowcount or 0)
    finally:
        con.close()


def list_rows(table: str, *, limit: int = 50, where: str = '', args: Optional[List[Any]] = None, order_by: str = 'updated_ts DESC') -> List[Dict[str, Any]]:
    ensure_schema()
    args = list(args or [])
    limit = max(1, min(int(limit), 500))
    q = f"SELECT * FROM {table}"
    if where:
        q += f" WHERE {where}"
    if order_by:
        q += f" ORDER BY {order_by}"
    q += " LIMIT ?"
    args.append(limit)
    con = _connect()
    try:
        rows = con.execute(q, args).fetchall()
        return [dict(r) for r in rows]
    finally:
        con.close()


def unresolved_anomalies(limit: int = 50) -> List[Dict[str, Any]]:
    return list_rows('anomalies', limit=limit, where='resolved_ts IS NULL', order_by='created_ts DESC')


def link_trade_plan_to_intent_atomic(intent_id: str, trade_plan: Dict[str, Any], *, intent_fields: Optional[Dict[str, Any]] = None) -> None:
    ensure_schema()
    plan_payload = _prepare_trade_plan_payload(trade_plan)
    intent_updates = dict(intent_fields or {})
    intent_updates['trade_plan_id'] = str(plan_payload.get('trade_plan_id') or '')
    intent_updates['updated_ts'] = time.time()
    con = _connect()
    try:
        _execute_trade_plan_upsert(con, plan_payload)
        sets = []
        args: List[Any] = []
        for k, v in intent_updates.items():
            if k in {'trade_plan_id','state','broker_txid','filled_qty','avg_fill_price','fees_usd','retry_count','reject_reason','cancel_reason','last_broker_status','remaining_qty','submitted_ts','acknowledged_ts','raw_json','updated_ts'}:
                sets.append(f"{k} = ?")
                if k == 'raw_json' and not isinstance(v, str):
                    v = _json(v)
                args.append(v)
        if sets:
            args.append(intent_id)
            con.execute(f"UPDATE order_intents SET {', '.join(sets)} WHERE intent_id = ?", args)
        con.commit()
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()



def upsert_broker_order(order: Dict[str, Any]) -> None:
    ensure_schema()
    now = time.time()
    payload = dict(order or {})
    payload.setdefault('status', 'created')
    payload.setdefault('created_ts', now)
    payload['updated_ts'] = now
    payload['reject_reason'] = normalize_terminal_reason(payload.get('reject_reason'))
    payload.setdefault('raw_json', _json(payload.get('raw_json') or order))
    payload = _sanitize_payload(payload, json_fields={'raw_json'})
    con = _connect()
    try:
        con.execute(
            """
            INSERT INTO broker_orders (
                broker_order_id, intent_id, trade_plan_id, symbol, strategy_id, side, order_type, lifecycle_stage, status,
                client_order_key, broker_txid, requested_qty, requested_notional_usd, limit_price, avg_fill_price,
                filled_qty, remaining_qty, fees_usd, reject_reason, raw_json, created_ts, updated_ts, acknowledged_ts, closed_ts
            ) VALUES (
                :broker_order_id, :intent_id, :trade_plan_id, :symbol, :strategy_id, :side, :order_type, :lifecycle_stage, :status,
                :client_order_key, :broker_txid, :requested_qty, :requested_notional_usd, :limit_price, :avg_fill_price,
                :filled_qty, :remaining_qty, :fees_usd, :reject_reason, :raw_json, :created_ts, :updated_ts, :acknowledged_ts, :closed_ts
            )
            ON CONFLICT(broker_order_id) DO UPDATE SET
                intent_id=excluded.intent_id,
                trade_plan_id=excluded.trade_plan_id,
                symbol=excluded.symbol,
                strategy_id=excluded.strategy_id,
                side=excluded.side,
                order_type=excluded.order_type,
                lifecycle_stage=excluded.lifecycle_stage,
                status=excluded.status,
                client_order_key=excluded.client_order_key,
                broker_txid=excluded.broker_txid,
                requested_qty=excluded.requested_qty,
                requested_notional_usd=excluded.requested_notional_usd,
                limit_price=excluded.limit_price,
                avg_fill_price=excluded.avg_fill_price,
                filled_qty=excluded.filled_qty,
                remaining_qty=excluded.remaining_qty,
                fees_usd=excluded.fees_usd,
                reject_reason=excluded.reject_reason,
                raw_json=excluded.raw_json,
                updated_ts=excluded.updated_ts,
                acknowledged_ts=excluded.acknowledged_ts,
                closed_ts=excluded.closed_ts
            """,
            payload,
        )
        con.commit()
    finally:
        con.close()


def record_trade_lifecycle_event(*, stage: str, event_type: str, trade_plan_id: str | None = None, intent_id: str | None = None, broker_order_id: str | None = None, position_id: str | None = None, symbol: str | None = None, strategy_id: str | None = None, reason: str | None = None, payload: Optional[Dict[str, Any]] = None, event_id: str | None = None) -> str:
    ensure_schema()
    ts = time.time()
    event_id = str(event_id or f"{int(ts*1000)}:{stage}:{event_type}:{trade_plan_id or intent_id or symbol or ''}")
    con = _connect()
    try:
        con.execute(
            """
            INSERT OR REPLACE INTO trade_lifecycle_events(event_id, trade_plan_id, intent_id, broker_order_id, position_id, symbol, strategy_id, stage, event_type, reason, payload_json, created_ts)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (event_id, trade_plan_id, intent_id, broker_order_id, position_id, symbol, strategy_id, stage, event_type, reason, _json(payload or {}), ts),
        )
        con.commit()
        return event_id
    finally:
        con.close()


def list_recent_broker_orders(*, symbol: str | None = None, strategy_id: str | None = None, status: str | None = None, since_ts: float | None = None, limit: int = 100) -> List[Dict[str, Any]]:
    ensure_schema()
    clauses = []
    args: List[Any] = []
    if symbol is not None:
        clauses.append('symbol = ?')
        args.append(symbol)
    if strategy_id is not None:
        clauses.append('strategy_id = ?')
        args.append(strategy_id)
    if status is not None:
        clauses.append('status = ?')
        args.append(status)
    if since_ts is not None:
        clauses.append('created_ts >= ?')
        args.append(float(since_ts))
    where = ' AND '.join(clauses)
    return list_rows('broker_orders', limit=limit, where=where, args=args, order_by='updated_ts DESC')


def summarize_broker_orders(*, since_ts: float | None = None) -> Dict[str, Any]:
    ensure_schema()
    clauses = []
    args: List[Any] = []
    if since_ts is not None:
        clauses.append('created_ts >= ?')
        args.append(float(since_ts))
    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ''
    con = _connect()
    try:
        rows = con.execute(
            f"SELECT status, COALESCE(lifecycle_stage, '') AS lifecycle_stage, COUNT(*) AS n FROM broker_orders {where_sql} GROUP BY status, COALESCE(lifecycle_stage, '')",
            tuple(args),
        ).fetchall()
        by_status: Dict[str, int] = {}
        by_stage: Dict[str, int] = {}
        total = 0
        for row in rows:
            status = str(row['status'] or '')
            stage = str(row['lifecycle_stage'] or '')
            n = int(row['n'] or 0)
            total += n
            by_status[status] = by_status.get(status, 0) + n
            if stage:
                by_stage[stage] = by_stage.get(stage, 0) + n
        return {'total': total, 'by_status': by_status, 'by_stage': by_stage}
    finally:
        con.close()


def list_recent_trade_lifecycle_events(*, symbol: str | None = None, trade_plan_id: str | None = None, stage: str | None = None, since_ts: float | None = None, limit: int = 100) -> List[Dict[str, Any]]:
    ensure_schema()
    clauses = []
    args: List[Any] = []
    if symbol is not None:
        clauses.append('symbol = ?')
        args.append(symbol)
    if trade_plan_id is not None:
        clauses.append('trade_plan_id = ?')
        args.append(trade_plan_id)
    if stage is not None:
        clauses.append('stage = ?')
        args.append(stage)
    if since_ts is not None:
        clauses.append('created_ts >= ?')
        args.append(float(since_ts))
    where = ' AND '.join(clauses)
    return list_rows('trade_lifecycle_events', limit=limit, where=where, args=args, order_by='created_ts DESC')


def list_trade_lifecycle_events(*, symbol: str | None = None, strategy_id: str | None = None, trade_plan_id: str | None = None, stage: str | None = None, since_ts: float | None = None, limit: int = 100) -> List[Dict[str, Any]]:
    ensure_schema()
    clauses = []
    args: List[Any] = []
    if symbol is not None:
        clauses.append('symbol = ?')
        args.append(symbol)
    if strategy_id is not None:
        clauses.append('strategy_id = ?')
        args.append(strategy_id)
    if trade_plan_id is not None:
        clauses.append('trade_plan_id = ?')
        args.append(trade_plan_id)
    if stage is not None:
        clauses.append('stage = ?')
        args.append(stage)
    if since_ts is not None:
        clauses.append('created_ts >= ?')
        args.append(float(since_ts))
    where = ' AND '.join(clauses)
    return list_rows('trade_lifecycle_events', limit=limit, where=where, args=args, order_by='created_ts DESC')


def summarize_trade_lifecycle(*, since_ts: float | None = None) -> Dict[str, Any]:
    ensure_schema()
    clauses = []
    args: List[Any] = []
    if since_ts is not None:
        clauses.append('created_ts >= ?')
        args.append(float(since_ts))
    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ''
    con = _connect()
    try:
        rows = con.execute(
            f"SELECT stage, event_type, COUNT(*) AS n FROM trade_lifecycle_events {where_sql} GROUP BY stage, event_type",
            tuple(args),
        ).fetchall()
        by_stage: Dict[str, int] = {}
        by_event_type: Dict[str, int] = {}
        total = 0
        for row in rows:
            stage = str(row['stage'] or '')
            event_type = str(row['event_type'] or '')
            n = int(row['n'] or 0)
            total += n
            by_stage[stage] = by_stage.get(stage, 0) + n
            by_event_type[event_type] = by_event_type.get(event_type, 0) + n
        return {'total': total, 'by_stage': by_stage, 'by_event_type': by_event_type}
    finally:
        con.close()


def record_ops_event(event_type: str, *, symbol: str | None = None, strategy_id: str | None = None, signal_id: str | None = None, fingerprint: str | None = None, reason: str | None = None, payload: Optional[Dict[str, Any]] = None, event_id: str | None = None) -> str:
    ensure_schema()
    reason = normalize_terminal_reason(reason)
    payload_obj = dict(payload or {})
    payload_obj['reason'] = reason
    event_id = str(event_id or f"{int(time.time()*1000)}:{event_type}:{symbol or ''}:{strategy_id or ''}")
    con = _connect()
    try:
        con.execute(
            """
            INSERT OR REPLACE INTO ops_events(event_id, event_type, symbol, strategy_id, signal_id, fingerprint, reason, payload_json, created_ts)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (event_id, str(event_type or ''), symbol, strategy_id, signal_id, fingerprint, reason, _json(payload_obj), time.time()),
        )
        con.commit()
        return event_id
    finally:
        con.close()


def list_recent_ops_events(*, symbol: str | None = None, strategy_id: str | None = None, event_types: Optional[List[str]] = None, since_ts: float | None = None, limit: int = 100) -> List[Dict[str, Any]]:
    ensure_schema()
    clauses = []
    args: List[Any] = []
    if symbol is not None:
        clauses.append('symbol = ?')
        args.append(symbol)
    if strategy_id is not None:
        clauses.append('strategy_id = ?')
        args.append(strategy_id)
    if event_types:
        clauses.append(f"event_type IN ({','.join('?' for _ in event_types)})")
        args.extend(list(event_types))
    if since_ts is not None:
        clauses.append('created_ts >= ?')
        args.append(float(since_ts))
    where = ' AND '.join(clauses)
    return list_rows('ops_events', limit=limit, where=where, args=args, order_by='created_ts DESC')


def acquire_workflow_lock(lock_key: str, *, symbol: str, strategy_id: str | None = None, stage: str | None = None, owner_req_id: str | None = None, ttl_sec: int = 300, metadata: Optional[Dict[str, Any]] = None) -> bool:
    ensure_schema()
    now = time.time()
    ttl = max(1, int(ttl_sec or 300))
    expires_ts = now + float(ttl)
    con = _connect()
    try:
        row = con.execute('SELECT * FROM workflow_locks WHERE lock_key = ?', (lock_key,)).fetchone()
        if row:
            released_ts = float(row['released_ts'] or 0.0) if row['released_ts'] is not None else 0.0
            existing_expires = float(row['expires_ts'] or 0.0) if row['expires_ts'] is not None else 0.0
            if released_ts <= 0.0 and existing_expires > now:
                return False
        con.execute(
            """
            INSERT INTO workflow_locks(lock_key, symbol, strategy_id, stage, owner_req_id, created_ts, expires_ts, released_ts, metadata_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, NULL, ?)
            ON CONFLICT(lock_key) DO UPDATE SET
                symbol=excluded.symbol,
                strategy_id=excluded.strategy_id,
                stage=excluded.stage,
                owner_req_id=excluded.owner_req_id,
                created_ts=excluded.created_ts,
                expires_ts=excluded.expires_ts,
                released_ts=NULL,
                metadata_json=excluded.metadata_json
            """,
            (lock_key, symbol, strategy_id, stage, owner_req_id, now, expires_ts, _json(metadata or {})),
        )
        con.commit()
        return True
    finally:
        con.close()


def release_workflow_lock(lock_key: str) -> None:
    ensure_schema()
    con = _connect()
    try:
        con.execute('UPDATE workflow_locks SET released_ts = ? WHERE lock_key = ? AND released_ts IS NULL', (time.time(), lock_key))
        con.commit()
    finally:
        con.close()


def sweep_expired_workflow_locks(*, now_ts: float | None = None, limit: int | None = None) -> int:
    ensure_schema()
    now_ts = float(now_ts or time.time())
    con = _connect()
    try:
        if limit is None:
            cur = con.execute(
                'UPDATE workflow_locks SET released_ts = ? WHERE released_ts IS NULL AND expires_ts IS NOT NULL AND expires_ts <= ?',
                (now_ts, now_ts),
            )
        else:
            lock_rows = con.execute(
                'SELECT lock_key FROM workflow_locks WHERE released_ts IS NULL AND expires_ts IS NOT NULL AND expires_ts <= ? ORDER BY created_ts ASC LIMIT ?',
                (now_ts, int(limit)),
            ).fetchall()
            keys = [str(r["lock_key"]) for r in lock_rows if r and r["lock_key"]]
            if not keys:
                return 0
            placeholders = ','.join('?' for _ in keys)
            cur = con.execute(
                f'UPDATE workflow_locks SET released_ts = ? WHERE released_ts IS NULL AND lock_key IN ({placeholders})',
                (now_ts, *keys),
            )
        con.commit()
        return int(getattr(cur, 'rowcount', 0) or 0)
    finally:
        con.close()


def get_active_workflow_lock(lock_key: str) -> Optional[Dict[str, Any]]:
    ensure_schema()
    now = time.time()
    sweep_expired_workflow_locks(now_ts=now, limit=100)
    con = _connect()
    try:
        row = con.execute(
            'SELECT * FROM workflow_locks WHERE lock_key = ? AND released_ts IS NULL AND expires_ts > ? LIMIT 1',
            (lock_key, now),
        ).fetchone()
        return dict(row) if row else None
    finally:
        con.close()


def count_open_positions_for_symbol(symbol: str) -> int:
    ensure_schema()
    con = _connect()
    try:
        row = con.execute("SELECT COUNT(*) AS n FROM position_ledger WHERE symbol = ? AND status = 'open'", (symbol,)).fetchone()
        return int((row['n'] if row else 0) or 0)
    finally:
        con.close()


def count_openish_trade_plans_for_symbol(symbol: str, strategy_id: str | None = None) -> int:
    ensure_schema()
    args: List[Any] = [symbol, *sorted(OPENISH_TRADE_PLAN_STATUSES)]
    where = f"symbol = ? AND status IN ({','.join('?' for _ in OPENISH_TRADE_PLAN_STATUSES)}) AND closed_ts IS NULL"
    if strategy_id:
        where += ' AND strategy_id = ?'
        args.append(strategy_id)
    con = _connect()
    try:
        row = con.execute(f"SELECT COUNT(*) AS n FROM trade_plans WHERE {where}", tuple(args)).fetchone()
        return int((row['n'] if row else 0) or 0)
    finally:
        con.close()


def count_openish_order_intents_for_symbol(symbol: str, strategy_id: str | None = None) -> int:
    ensure_schema()
    args: List[Any] = [symbol, *sorted(OPENISH_ORDER_INTENT_STATES)]
    where = f"symbol = ? AND state IN ({','.join('?' for _ in OPENISH_ORDER_INTENT_STATES)})"
    if strategy_id:
        where += ' AND strategy_id = ?'
        args.append(strategy_id)
    con = _connect()
    try:
        row = con.execute(f"SELECT COUNT(*) AS n FROM order_intents WHERE {where}", tuple(args)).fetchone()
        return int((row['n'] if row else 0) or 0)
    finally:
        con.close()


def can_start_new_entry(symbol: str, strategy_id: str, *, cooldown_sec: int = 0) -> Dict[str, Any]:
    ensure_schema()
    now = time.time()
    sweep_expired_workflow_locks(now_ts=now, limit=100)
    lock_key = f"entry:{symbol}:{strategy_id}"
    active_lock = get_active_workflow_lock(lock_key)
    if active_lock:
        return {'ok': False, 'reason': 'active_workflow_lock', 'blocking_objects': {'workflow_lock': active_lock}, 'cooldown_remaining_sec': max(0, int(float(active_lock.get('expires_ts') or 0.0) - time.time()))}
    open_plans = count_openish_trade_plans_for_symbol(symbol, strategy_id)
    if open_plans > 0:
        return {'ok': False, 'reason': 'open_trade_plan_exists', 'blocking_objects': {'open_trade_plans': open_plans}, 'cooldown_remaining_sec': 0}
    open_intents = count_openish_order_intents_for_symbol(symbol, strategy_id)
    if open_intents > 0:
        return {'ok': False, 'reason': 'open_order_intent_exists', 'blocking_objects': {'open_order_intents': open_intents}, 'cooldown_remaining_sec': 0}
    open_positions = count_open_positions_for_symbol(symbol)
    if open_positions > 0:
        return {'ok': False, 'reason': 'open_position_exists', 'blocking_objects': {'open_positions': open_positions}, 'cooldown_remaining_sec': 0}
    if int(cooldown_sec or 0) > 0:
        recent = list_recent_ops_events(symbol=symbol, strategy_id=strategy_id, event_types=['entry_submit_failed','entry_broker_rejected','entry_broker_cancelled','entry_failed'], since_ts=time.time() - float(cooldown_sec), limit=1)
        if recent:
            evt = recent[0]
            remaining = max(0, int((float(evt.get('created_ts') or 0.0) + float(cooldown_sec)) - time.time()))
            return {'ok': False, 'reason': 'entry_failure_cooldown', 'blocking_objects': {'recent_failure_event': evt}, 'cooldown_remaining_sec': remaining}
    return {'ok': True, 'reason': '', 'blocking_objects': {}, 'cooldown_remaining_sec': 0}



def purge_expired_signal_fingerprints(now_ts: float | None = None) -> int:
    ensure_schema()
    now_ts = float(now_ts or time.time())
    con = _connect()
    try:
        cur = con.execute('DELETE FROM signal_fingerprints WHERE expires_ts IS NOT NULL AND expires_ts <= ?', (now_ts,))
        con.commit()
        return int(getattr(cur, 'rowcount', 0) or 0)
    finally:
        con.close()


def get_active_signal_fingerprint(fingerprint: str) -> Optional[Dict[str, Any]]:
    ensure_schema()
    now_ts = time.time()
    con = _connect()
    try:
        row = con.execute(
            'SELECT * FROM signal_fingerprints WHERE fingerprint = ? AND (expires_ts IS NULL OR expires_ts > ?) LIMIT 1',
            (str(fingerprint or ''), now_ts),
        ).fetchone()
        return dict(row) if row else None
    finally:
        con.close()


def register_signal_fingerprint(fingerprint: str, *, symbol: str | None = None, strategy_id: str | None = None, signal_id: str | None = None, ttl_sec: int = 0, metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    ensure_schema()
    fp = str(fingerprint or '').strip()
    if not fp:
        return {'ok': False, 'duplicate': False, 'reason': 'missing_fingerprint'}
    now_ts = time.time()
    purge_expired_signal_fingerprints(now_ts)
    ttl = max(1, int(ttl_sec or 1))
    expires_ts = now_ts + float(ttl)
    con = _connect()
    try:
        row = con.execute(
            'SELECT * FROM signal_fingerprints WHERE fingerprint = ? AND (expires_ts IS NULL OR expires_ts > ?) LIMIT 1',
            (fp, now_ts),
        ).fetchone()
        if row:
            con.execute(
                'UPDATE signal_fingerprints SET last_seen_ts = ?, expires_ts = ? WHERE fingerprint = ?',
                (now_ts, max(float(row['expires_ts'] or 0.0), expires_ts), fp),
            )
            con.commit()
            return {'ok': True, 'duplicate': True, 'existing': dict(row)}
        con.execute(
            """
            INSERT INTO signal_fingerprints(fingerprint, symbol, strategy_id, signal_id, first_seen_ts, last_seen_ts, expires_ts, metadata_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (fp, symbol, strategy_id, signal_id, now_ts, now_ts, expires_ts, _json(metadata or {})),
        )
        con.commit()
        return {'ok': True, 'duplicate': False, 'expires_ts': expires_ts}
    finally:
        con.close()


def list_active_workflow_locks(*, symbol: str | None = None, strategy_id: str | None = None, limit: int = 100) -> List[Dict[str, Any]]:
    ensure_schema()
    now = time.time()
    sweep_expired_workflow_locks(now_ts=now, limit=100)
    clauses = ['released_ts IS NULL', 'expires_ts > ?']
    args: List[Any] = [now]
    if symbol is not None:
        clauses.append('symbol = ?')
        args.append(symbol)
    if strategy_id is not None:
        clauses.append('strategy_id = ?')
        args.append(strategy_id)
    where = ' AND '.join(clauses)
    return list_rows('workflow_locks', limit=limit, where=where, args=args, order_by='created_ts DESC')


def backfill_legacy_trade_lifecycle_events(*, limit: int = 1000) -> int:
    ensure_schema()
    now = time.time()
    con = _connect()
    try:
        rows = con.execute(
            """
            SELECT tp.*
            FROM trade_plans tp
            LEFT JOIN trade_lifecycle_events tle ON tle.trade_plan_id = tp.trade_plan_id
            WHERE tle.event_id IS NULL
            ORDER BY COALESCE(tp.closed_ts, tp.updated_ts, tp.created_ts) ASC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()
        inserted = 0
        for row in rows:
            rec = dict(row)
            status = str(rec.get('status') or '').strip()
            trade_plan_id = str(rec.get('trade_plan_id') or '').strip()
            if not trade_plan_id:
                continue
            if status in TERMINAL_TRADE_PLAN_STATUSES:
                event_type = 'legacy_terminal_backfill'
                stage = 'legacy_terminal'
            elif status in OPENISH_TRADE_PLAN_STATUSES:
                event_type = 'legacy_open_backfill'
                stage = 'legacy_open'
            else:
                event_type = 'legacy_trade_plan_backfill'
                stage = 'legacy'
            reason = normalize_terminal_reason(status, default='legacy_backfill') or 'legacy_backfill'
            event_ts = float(rec.get('closed_ts') or rec.get('updated_ts') or rec.get('created_ts') or now)
            payload = {
                'legacy_backfill': True,
                'original_status': status,
                'created_ts': rec.get('created_ts'),
                'updated_ts': rec.get('updated_ts'),
                'closed_ts': rec.get('closed_ts'),
                'expires_ts': rec.get('expires_ts'),
            }
            con.execute(
                """
                INSERT OR IGNORE INTO trade_lifecycle_events(
                    event_id, trade_plan_id, intent_id, broker_order_id, position_id,
                    symbol, strategy_id, stage, event_type, reason, payload_json, created_ts
                ) VALUES (?, ?, NULL, NULL, NULL, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    f"legacy:{trade_plan_id}",
                    trade_plan_id,
                    rec.get('symbol'),
                    rec.get('strategy_id'),
                    stage,
                    event_type,
                    reason,
                    _json(payload),
                    event_ts,
                ),
            )
            inserted += 1
        con.commit()
        return inserted
    finally:
        con.close()


def repair_lifecycle_integrity(*, stale_age_sec: int = 900, backfill_limit: int = 1000) -> Dict[str, int]:
    ensure_schema()
    now = time.time()
    return {
        'expired_workflow_locks_released': sweep_expired_workflow_locks(now_ts=now),
        'legacy_trade_lifecycle_events_backfilled': backfill_legacy_trade_lifecycle_events(limit=backfill_limit),
        'expired_signal_fingerprints_purged': purge_expired_signal_fingerprints(now),
    }


def record_admission_event(*, symbol: str, strategy_id: str | None = None, signal_id: str | None = None, signal_name: str | None = None, fingerprint: str | None = None, source: str | None = None, bar_ts: int | None = None, trigger_price: float | None = None, reference_price: float | None = None, atr: float | None = None, range_high: float | None = None, range_low: float | None = None, range_width_pct: float | None = None, breakout_level: float | None = None, breakout_distance_pct: float | None = None, ranking_score: float | None = None, spread_pct: float | None = None, regime_state: str | None = None, admission_result: str | None = None, reject_reason: str | None = None, payload: Optional[Dict[str, Any]] = None, event_id: str | None = None) -> str:
    ensure_schema()
    ts = time.time()
    event_id = str(event_id or f"{int(ts*1000)}:adm:{symbol}:{strategy_id or ''}")
    reject_reason = normalize_terminal_reason(reject_reason)
    payload_obj = dict(payload or {})
    if reject_reason:
        payload_obj['reject_reason'] = reject_reason
    con = _connect()
    try:
        con.execute(
            """
            INSERT OR REPLACE INTO admission_events(event_id, symbol, strategy_id, signal_id, signal_name, fingerprint, source, bar_ts, trigger_price, reference_price, atr, range_high, range_low, range_width_pct, breakout_level, breakout_distance_pct, ranking_score, spread_pct, regime_state, admission_result, reject_reason, payload_json, created_ts)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (event_id, symbol, strategy_id, signal_id, signal_name, fingerprint, source, bar_ts, trigger_price, reference_price, atr, range_high, range_low, range_width_pct, breakout_level, breakout_distance_pct, ranking_score, spread_pct, regime_state, admission_result, reject_reason, _json(payload_obj), ts),
        )
        con.commit()
        return event_id
    finally:
        con.close()


def list_recent_admission_events(*, symbol: str | None = None, strategy_id: str | None = None, admission_result: str | None = None, since_ts: float | None = None, limit: int = 100) -> List[Dict[str, Any]]:
    ensure_schema()
    clauses = []
    args: List[Any] = []
    if symbol is not None:
        clauses.append('symbol = ?')
        args.append(symbol)
    if strategy_id is not None:
        clauses.append('strategy_id = ?')
        args.append(strategy_id)
    if admission_result is not None:
        clauses.append('admission_result = ?')
        args.append(admission_result)
    if since_ts is not None:
        clauses.append('created_ts >= ?')
        args.append(float(since_ts))
    where = ' AND '.join(clauses)
    return list_rows('admission_events', limit=limit, where=where, args=args, order_by='created_ts DESC')


def summarize_admission_events(*, since_ts: float | None = None) -> Dict[str, Any]:
    ensure_schema()
    clauses = []
    args: List[Any] = []
    if since_ts is not None:
        clauses.append('created_ts >= ?')
        args.append(float(since_ts))
    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ''
    con = _connect()
    try:
        rows = con.execute(
            f"SELECT admission_result, COALESCE(reject_reason, '') AS reject_reason, COUNT(*) AS n FROM admission_events {where_sql} GROUP BY admission_result, COALESCE(reject_reason, '')",
            tuple(args),
        ).fetchall()
        by_result: Dict[str, int] = {}
        by_reason: Dict[str, int] = {}
        total = 0
        for row in rows:
            result = str(row['admission_result'] or '')
            reason = str(row['reject_reason'] or '')
            n = int(row['n'] or 0)
            total += n
            by_result[result] = by_result.get(result, 0) + n
            if reason:
                by_reason[reason] = by_reason.get(reason, 0) + n
        return {'total': total, 'by_result': by_result, 'by_reason': by_reason}
    finally:
        con.close()


def summarize_ops_events(*, since_ts: float | None = None) -> Dict[str, Any]:
    ensure_schema()
    clauses = []
    args: List[Any] = []
    if since_ts is not None:
        clauses.append('created_ts >= ?')
        args.append(float(since_ts))
    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ''
    con = _connect()
    try:
        rows = con.execute(
            f"SELECT event_type, COUNT(*) AS n FROM ops_events {where_sql} GROUP BY event_type",
            tuple(args),
        ).fetchall()
        by_type: Dict[str, int] = {}
        total = 0
        for row in rows:
            et = str(row['event_type'] or '')
            n = int(row['n'] or 0)
            total += n
            by_type[et] = n
        return {'total': total, 'by_type': by_type}
    finally:
        con.close()


def summarize_terminal_reasons(*, since_ts: float | None = None) -> Dict[str, Any]:
    ensure_schema()
    con = _connect()
    try:
        sources = {
            'order_intents': ('SELECT COALESCE(reject_reason, cancel_reason, "") AS reason, COUNT(*) AS n FROM order_intents WHERE COALESCE(reject_reason, cancel_reason, "") <> "" {where} GROUP BY COALESCE(reject_reason, cancel_reason, "")', 'updated_ts'),
            'broker_orders': ('SELECT COALESCE(reject_reason, "") AS reason, COUNT(*) AS n FROM broker_orders WHERE COALESCE(reject_reason, "") <> "" {where} GROUP BY COALESCE(reject_reason, "")', 'updated_ts'),
            'trade_lifecycle_events': ('SELECT COALESCE(reason, "") AS reason, COUNT(*) AS n FROM trade_lifecycle_events WHERE COALESCE(reason, "") <> "" {where} GROUP BY COALESCE(reason, "")', 'created_ts'),
            'ops_events': ('SELECT COALESCE(reason, "") AS reason, COUNT(*) AS n FROM ops_events WHERE COALESCE(reason, "") <> "" {where} GROUP BY COALESCE(reason, "")', 'created_ts'),
            'admission_events': ('SELECT COALESCE(reject_reason, "") AS reason, COUNT(*) AS n FROM admission_events WHERE COALESCE(reject_reason, "") <> "" {where} GROUP BY COALESCE(reject_reason, "")', 'created_ts'),
        }
        overall: Dict[str, int] = {}
        by_source: Dict[str, Dict[str, int]] = {}
        for name, (sql, ts_col) in sources.items():
            args: List[Any] = []
            where = ''
            if since_ts is not None:
                where = f' AND {ts_col} >= ?'
                args.append(float(since_ts))
            rows = con.execute(sql.format(where=where), tuple(args)).fetchall()
            src: Dict[str, int] = {}
            for row in rows:
                reason = normalize_terminal_reason(row['reason']) or ''
                n = int(row['n'] or 0)
                if not reason:
                    continue
                src[reason] = src.get(reason, 0) + n
                overall[reason] = overall.get(reason, 0) + n
            by_source[name] = src
        return {'overall': overall, 'by_source': by_source}
    finally:
        con.close()


def lifecycle_integrity_report(*, limit: int = 100, stale_age_sec: int = 900) -> Dict[str, Any]:
    ensure_schema()
    repairs = repair_lifecycle_integrity(stale_age_sec=stale_age_sec, backfill_limit=max(int(limit or 100), 1000))
    now = time.time()
    con = _connect()
    try:
        report: Dict[str, Any] = {'ok': True, 'checked_ts': now, 'stale_age_sec': int(stale_age_sec), 'repairs': repairs, 'counts': {}, 'samples': {}}
        checks = {
            'broker_orders_missing_intent': "SELECT bo.* FROM broker_orders bo LEFT JOIN order_intents oi ON oi.intent_id = bo.intent_id WHERE COALESCE(bo.intent_id, '') <> '' AND oi.intent_id IS NULL ORDER BY bo.updated_ts DESC LIMIT ?",
            'trade_plans_missing_lifecycle': "SELECT tp.* FROM trade_plans tp LEFT JOIN trade_lifecycle_events tle ON tle.trade_plan_id = tp.trade_plan_id WHERE tle.event_id IS NULL ORDER BY tp.updated_ts DESC LIMIT ?",
            'open_positions_missing_trade_plan': "SELECT pl.* FROM position_ledger pl LEFT JOIN trade_plans tp ON tp.trade_plan_id = pl.trade_plan_id WHERE pl.status = 'open' AND (COALESCE(pl.trade_plan_id, '') = '' OR tp.trade_plan_id IS NULL) ORDER BY pl.updated_ts DESC LIMIT ?",
            'terminal_trade_plans_missing_closed_ts': "SELECT * FROM trade_plans WHERE status IN ({statuses}) AND closed_ts IS NULL ORDER BY updated_ts DESC LIMIT ?".format(statuses=','.join('?' for _ in TERMINAL_TRADE_PLAN_STATUSES)),
            'openish_trade_plans_expired': "SELECT * FROM trade_plans WHERE status IN ({statuses}) AND closed_ts IS NULL AND expires_ts IS NOT NULL AND expires_ts < ? ORDER BY updated_ts DESC LIMIT ?".format(statuses=','.join('?' for _ in OPENISH_TRADE_PLAN_STATUSES)),
            'stale_openish_order_intents': "SELECT * FROM order_intents WHERE state IN ({states}) AND updated_ts < ? ORDER BY updated_ts DESC LIMIT ?".format(states=','.join('?' for _ in OPENISH_ORDER_INTENT_STATES)),
            'active_workflow_locks_expired': "SELECT * FROM workflow_locks WHERE released_ts IS NULL AND expires_ts <= ? ORDER BY created_ts DESC LIMIT ?",
        }
        # simple checks
        rows = con.execute(checks['broker_orders_missing_intent'], (limit,)).fetchall()
        report['counts']['broker_orders_missing_intent'] = len(rows)
        report['samples']['broker_orders_missing_intent'] = [dict(r) for r in rows]
        rows = con.execute(checks['trade_plans_missing_lifecycle'], (limit,)).fetchall()
        report['counts']['trade_plans_missing_lifecycle'] = len(rows)
        report['samples']['trade_plans_missing_lifecycle'] = [dict(r) for r in rows]
        rows = con.execute(checks['open_positions_missing_trade_plan'], (limit,)).fetchall()
        report['counts']['open_positions_missing_trade_plan'] = len(rows)
        report['samples']['open_positions_missing_trade_plan'] = [dict(r) for r in rows]
        args = list(sorted(TERMINAL_TRADE_PLAN_STATUSES)) + [limit]
        rows = con.execute(checks['terminal_trade_plans_missing_closed_ts'], tuple(args)).fetchall()
        report['counts']['terminal_trade_plans_missing_closed_ts'] = len(rows)
        report['samples']['terminal_trade_plans_missing_closed_ts'] = [dict(r) for r in rows]
        args = list(sorted(OPENISH_TRADE_PLAN_STATUSES)) + [now, limit]
        rows = con.execute(checks['openish_trade_plans_expired'], tuple(args)).fetchall()
        report['counts']['openish_trade_plans_expired'] = len(rows)
        report['samples']['openish_trade_plans_expired'] = [dict(r) for r in rows]
        args = list(sorted(OPENISH_ORDER_INTENT_STATES)) + [now - float(stale_age_sec), limit]
        rows = con.execute(checks['stale_openish_order_intents'], tuple(args)).fetchall()
        report['counts']['stale_openish_order_intents'] = len(rows)
        report['samples']['stale_openish_order_intents'] = [dict(r) for r in rows]
        rows = con.execute(checks['active_workflow_locks_expired'], (now, limit)).fetchall()
        report['counts']['active_workflow_locks_expired'] = len(rows)
        report['samples']['active_workflow_locks_expired'] = [dict(r) for r in rows]
        report['ok'] = all(int(v or 0) == 0 for v in report['counts'].values())
        return report
    finally:
        con.close()


def summary() -> Dict[str, Any]:
    ensure_schema()
    con = _connect()
    try:
        def one(q: str, args: tuple = ()):
            row = con.execute(q, args).fetchone()
            return dict(row) if row else {}
        plans_total = one('SELECT COUNT(*) AS n FROM trade_plans').get('n', 0)
        plans_open = one(
            f"SELECT COUNT(*) AS n FROM trade_plans WHERE status IN ({','.join('?' for _ in OPENISH_TRADE_PLAN_STATUSES)}) AND closed_ts IS NULL",
            tuple(sorted(OPENISH_TRADE_PLAN_STATUSES)),
        ).get('n', 0)
        intents_open = one(
            f"SELECT COUNT(*) AS n FROM order_intents WHERE state IN ({','.join('?' for _ in OPENISH_ORDER_INTENT_STATES)})",
            tuple(sorted(OPENISH_ORDER_INTENT_STATES)),
        ).get('n', 0)
        positions_open = one("SELECT COUNT(*) AS n FROM position_ledger WHERE status = 'open'").get('n', 0)
        fills_total = one('SELECT COUNT(*) AS n FROM fill_events').get('n', 0)
        broker_orders_total = one('SELECT COUNT(*) AS n FROM broker_orders').get('n', 0)
        trade_lifecycle_total = one('SELECT COUNT(*) AS n FROM trade_lifecycle_events').get('n', 0)
        anomalies_open = one('SELECT COUNT(*) AS n FROM anomalies WHERE resolved_ts IS NULL').get('n', 0)
        return {
            'ok': True,
            'db_path': _db_path(),
            'trade_plans_total': int(plans_total or 0),
            'trade_plans_openish': int(plans_open or 0),
            'order_intents_openish': int(intents_open or 0),
            'positions_open': int(positions_open or 0),
            'fill_events_total': int(fills_total or 0),
            'broker_orders_total': int(broker_orders_total or 0),
            'trade_lifecycle_events_total': int(trade_lifecycle_total or 0),
            'anomalies_open': int(anomalies_open or 0),
        }
    finally:
        con.close()


def list_openish_trade_plans(limit: int = 200) -> List[Dict[str, Any]]:
    where = f"status IN ({','.join('?' for _ in OPENISH_TRADE_PLAN_STATUSES)}) AND closed_ts IS NULL"
    return list_rows(
        'trade_plans',
        limit=limit,
        where=where,
        args=list(sorted(OPENISH_TRADE_PLAN_STATUSES)),
        order_by='updated_ts DESC',
    )


def close_trade_plan(trade_plan_id: str, status: str = 'failed_reconcile', **fields: Any) -> None:
    payload = dict(fields or {})
    payload.setdefault('closed_ts', time.time())
    if status in OPENISH_TRADE_PLAN_STATUSES and status not in TERMINAL_TRADE_PLAN_STATUSES:
        status = 'failed_reconcile'
    update_trade_plan_status(trade_plan_id, status, **payload)


def list_openish_order_intents(limit: int = 200) -> List[Dict[str, Any]]:
    where = f"state IN ({','.join('?' for _ in OPENISH_ORDER_INTENT_STATES)})"
    return list_rows(
        'order_intents',
        limit=limit,
        where=where,
        args=list(sorted(OPENISH_ORDER_INTENT_STATES)),
        order_by='updated_ts DESC',
    )


def resolve_anomaly(anomaly_id: int) -> None:
    ensure_schema()
    con = _connect()
    try:
        con.execute('UPDATE anomalies SET resolved_ts = ? WHERE id = ? AND resolved_ts IS NULL', (time.time(), int(anomaly_id)))
        con.commit()
    finally:
        con.close()
