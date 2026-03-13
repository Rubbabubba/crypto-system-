from __future__ import annotations

import os
import sqlite3
from typing import Any, Dict
import json

DEFAULT_PLANS_DB_PATH = "/var/data/plans.sqlite3"


def _db_path() -> str:
    return (os.getenv("PLANS_DB_PATH") or DEFAULT_PLANS_DB_PATH).strip() or DEFAULT_PLANS_DB_PATH


def _connect() -> sqlite3.Connection:
    path = _db_path()
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    con = sqlite3.connect(path)
    con.row_factory = sqlite3.Row
    return con


def ensure_schema() -> None:
    con = _connect()
    try:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS plans (
                symbol TEXT PRIMARY KEY,
                side TEXT NOT NULL,
                notional_usd REAL NOT NULL,
                entry_price REAL NOT NULL,
                stop_price REAL NOT NULL,
                take_price REAL NOT NULL,
                strategy TEXT NOT NULL,
                opened_ts REAL NOT NULL,
                max_hold_sec INTEGER NOT NULL,
                breakeven_armed INTEGER NOT NULL,
                breakeven_triggered_ts REAL NOT NULL,
                trade_plan_id TEXT,
                position_id TEXT,
                signal_id TEXT,
                status TEXT,
                entry_mode TEXT,
                risk_snapshot_json TEXT
            )
            """
        )
        cols = {r["name"] for r in con.execute("PRAGMA table_info(plans)").fetchall()}
        wanted = {
            "trade_plan_id": "TEXT",
            "position_id": "TEXT",
            "signal_id": "TEXT",
            "status": "TEXT",
            "entry_mode": "TEXT",
            "risk_snapshot_json": "TEXT",
        }
        for name, ddl in wanted.items():
            if name not in cols:
                con.execute(f"ALTER TABLE plans ADD COLUMN {name} {ddl}")
        con.commit()
    finally:
        con.close()


def upsert_plan(plan_dict: Dict[str, Any]) -> None:
    """Persist a TradePlan (as a dict) to sqlite."""
    ensure_schema()
    plan_dict = dict(plan_dict or {})
    if not isinstance(plan_dict.get("risk_snapshot_json"), str):
        plan_dict["risk_snapshot_json"] = json.dumps(plan_dict.get("risk_snapshot_json") or {}, separators=(",", ":"), sort_keys=True)
    con = _connect()
    try:
        con.execute(
            """
            INSERT INTO plans (
                symbol, side, notional_usd, entry_price, stop_price, take_price,
                strategy, opened_ts, max_hold_sec, breakeven_armed, breakeven_triggered_ts,
                trade_plan_id, position_id, signal_id, status, entry_mode, risk_snapshot_json
            ) VALUES (
                :symbol, :side, :notional_usd, :entry_price, :stop_price, :take_price,
                :strategy, :opened_ts, :max_hold_sec, :breakeven_armed, :breakeven_triggered_ts,
                :trade_plan_id, :position_id, :signal_id, :status, :entry_mode, :risk_snapshot_json
            )
            ON CONFLICT(symbol) DO UPDATE SET
                side=excluded.side,
                notional_usd=excluded.notional_usd,
                entry_price=excluded.entry_price,
                stop_price=excluded.stop_price,
                take_price=excluded.take_price,
                strategy=excluded.strategy,
                opened_ts=excluded.opened_ts,
                max_hold_sec=excluded.max_hold_sec,
                breakeven_armed=excluded.breakeven_armed,
                breakeven_triggered_ts=excluded.breakeven_triggered_ts,
                trade_plan_id=excluded.trade_plan_id,
                position_id=excluded.position_id,
                signal_id=excluded.signal_id,
                status=excluded.status,
                entry_mode=excluded.entry_mode,
                risk_snapshot_json=excluded.risk_snapshot_json
            """,
            plan_dict,
        )
        con.commit()
    finally:
        con.close()


def delete_plan(symbol: str) -> None:
    ensure_schema()
    con = _connect()
    try:
        con.execute("DELETE FROM plans WHERE symbol = ?", (symbol,))
        con.commit()
    finally:
        con.close()


def load_plans() -> list[dict[str, Any]]:
    ensure_schema()
    con = _connect()
    try:
        rows = con.execute("SELECT * FROM plans").fetchall()
        out: list[dict[str, Any]] = []
        for r in rows:
            d = dict(r)
            d["breakeven_armed"] = bool(int(d.get("breakeven_armed") or 0))
            try:
                d["risk_snapshot_json"] = json.loads(d.get("risk_snapshot_json") or "{}")
            except Exception:
                d["risk_snapshot_json"] = {}
            out.append(d)
        return out
    finally:
        con.close()
