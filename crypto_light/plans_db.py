from __future__ import annotations

import os
import sqlite3
from typing import Any, Dict

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
                breakeven_triggered_ts REAL NOT NULL
            )
            """
        )
        con.commit()
    finally:
        con.close()


def upsert_plan(plan_dict: Dict[str, Any]) -> None:
    """Persist a TradePlan (as a dict) to sqlite."""
    ensure_schema()
    con = _connect()
    try:
        con.execute(
            """
            INSERT INTO plans (
                symbol, side, notional_usd, entry_price, stop_price, take_price,
                strategy, opened_ts, max_hold_sec, breakeven_armed, breakeven_triggered_ts
            ) VALUES (
                :symbol, :side, :notional_usd, :entry_price, :stop_price, :take_price,
                :strategy, :opened_ts, :max_hold_sec, :breakeven_armed, :breakeven_triggered_ts
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
                breakeven_triggered_ts=excluded.breakeven_triggered_ts
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
            out.append(d)
        return out
    finally:
        con.close()
