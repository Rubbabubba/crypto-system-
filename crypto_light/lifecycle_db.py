from __future__ import annotations

import json
import os
import sqlite3
import time
from typing import Any, Dict, List, Optional

DEFAULT_DB_PATH = "/var/data/lifecycle.sqlite3"


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
            CREATE UNIQUE INDEX IF NOT EXISTS idx_trade_plans_legacy_symbol_key ON trade_plans(legacy_symbol_key);
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
        _ensure_columns(con, 'order_intents', {
            'cancel_reason': 'TEXT',
            'client_order_key': 'TEXT',
            'last_broker_status': 'TEXT',
            'remaining_qty': 'REAL',
            'submitted_ts': 'REAL',
            'acknowledged_ts': 'REAL',
        })
        con.commit()
    finally:
        con.close()
    return _db_path()


def upsert_trade_plan(plan: Dict[str, Any]) -> None:
    ensure_schema()
    now = time.time()
    payload = dict(plan or {})
    payload.setdefault('status', 'created')
    payload.setdefault('created_ts', now)
    payload['updated_ts'] = now
    payload.setdefault('risk_snapshot_json', _json(payload.get('risk_snapshot_json') or payload.get('risk_snapshot') or {}))
    con = _connect()
    try:
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
    now = time.time()
    payload = dict(intent or {})
    payload.setdefault('state', 'created')
    payload.setdefault('created_ts', now)
    payload['updated_ts'] = now
    payload.setdefault('retry_count', 0)
    payload.setdefault('remaining_qty', payload.get('desired_qty'))
    payload.setdefault('submitted_ts', now if str(payload.get('state') or '') in {'submitted','acknowledged','filled','partial','cancel_pending','cancelled','failed_reconcile'} else None)
    payload.setdefault('acknowledged_ts', now if str(payload.get('state') or '') in {'acknowledged','filled','partial','cancel_pending','cancelled','failed_reconcile'} else None)
    payload.setdefault('raw_json', _json(payload.get('raw_json') or payload))
    con = _connect()
    try:
        con.execute(
            """
            INSERT INTO order_intents (
                intent_id, trade_plan_id, symbol, side, order_type, strategy_id, state,
                desired_qty, desired_notional_usd, limit_price, broker_txid,
                filled_qty, avg_fill_price, fees_usd, retry_count, reject_reason,
                cancel_reason, client_order_key, last_broker_status, remaining_qty,
                submitted_ts, acknowledged_ts, raw_json, created_ts, updated_ts
            ) VALUES (
                :intent_id, :trade_plan_id, :symbol, :side, :order_type, :strategy_id, :state,
                :desired_qty, :desired_notional_usd, :limit_price, :broker_txid,
                :filled_qty, :avg_fill_price, :fees_usd, :retry_count, :reject_reason,
                :cancel_reason, :client_order_key, :last_broker_status, :remaining_qty,
                :submitted_ts, :acknowledged_ts, :raw_json, :created_ts, :updated_ts
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
            payload,
        )
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


def summary() -> Dict[str, Any]:
    ensure_schema()
    con = _connect()
    try:
        def one(q: str, args: tuple = ()):
            row = con.execute(q, args).fetchone()
            return dict(row) if row else {}
        plans_total = one('SELECT COUNT(*) AS n FROM trade_plans').get('n', 0)
        plans_open = one("SELECT COUNT(*) AS n FROM trade_plans WHERE status IN ('approved','submitted','active')").get('n', 0)
        intents_open = one("SELECT COUNT(*) AS n FROM order_intents WHERE state IN ('created','validated','submitted','acknowledged','partial','replace_pending','cancel_pending')").get('n', 0)
        positions_open = one("SELECT COUNT(*) AS n FROM position_ledger WHERE status = 'open'").get('n', 0)
        fills_total = one('SELECT COUNT(*) AS n FROM fill_events').get('n', 0)
        anomalies_open = one('SELECT COUNT(*) AS n FROM anomalies WHERE resolved_ts IS NULL').get('n', 0)
        return {
            'ok': True,
            'db_path': _db_path(),
            'trade_plans_total': int(plans_total or 0),
            'trade_plans_openish': int(plans_open or 0),
            'order_intents_openish': int(intents_open or 0),
            'positions_open': int(positions_open or 0),
            'fill_events_total': int(fills_total or 0),
            'anomalies_open': int(anomalies_open or 0),
        }
    finally:
        con.close()


def list_openish_order_intents(limit: int = 200) -> List[Dict[str, Any]]:
    return list_rows(
        'order_intents',
        limit=limit,
        where="state IN ('created','validated','submitted','acknowledged','partial','replace_pending','cancel_pending','failed_reconcile')",
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
