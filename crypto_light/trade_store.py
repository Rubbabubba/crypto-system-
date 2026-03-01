"""Persistent trade history store (SQLite).

Render deployments are stateless by default. If you want to retain trade history
across deploys for the new trade/performance endpoints, mount a Render
Persistent Disk and set TRADE_DB_PATH to a path on that disk.

Example:
  TRADE_DB_PATH=/var/data/trades.sqlite3
"""

from __future__ import annotations

import json
import os
import sqlite3
import time
from typing import Any, Dict, List, Optional, Tuple


def _db_path() -> str:
    return os.getenv("TRADE_DB_PATH", ".data/trades.sqlite3")


def _connect() -> sqlite3.Connection:
    path = _db_path()
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with _connect() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS trades (
              txid TEXT PRIMARY KEY,
              pair TEXT,
              type TEXT,
              ordertype TEXT,
              price REAL,
              vol REAL,
              cost REAL,
              fee REAL,
              time REAL,
              misc TEXT,
              raw_json TEXT,
              inserted_utc REAL
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_time ON trades(time)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_pair_time ON trades(pair, time)")


def upsert_trades(trades: Any) -> Tuple[int, int]:
    """Upsert trades into SQLite.

    Accepts either:
      1) Dict[str, Dict] keyed by txid (raw Kraken shape), OR
      2) List[Dict] where each item has at least a 'txid' field.
    """
    if not trades:
        return (0, 0)

    init_db()
    now = time.time()

    if isinstance(trades, dict):
        items = [(str(txid), t) for txid, t in trades.items()]
    elif isinstance(trades, list):
        items = []
        for t in trades:
            if isinstance(t, dict) and t.get("txid"):
                items.append((str(t.get("txid")), t))
    else:
        items = []

    inserted = 0
    updated = 0

    with _connect() as conn:
        cur = conn.cursor()
        for txid, t in items:
            row = (
                txid,
                t.get("pair"),
                t.get("type"),
                t.get("ordertype"),
                _to_float(t.get("price")),
                _to_float(t.get("vol")),
                _to_float(t.get("cost")),
                _to_float(t.get("fee")),
                _to_float(t.get("time")),
                t.get("misc"),
                json.dumps(t, separators=(",", ":"), sort_keys=True),
                now,
            )
            try:
                cur.execute(
                    """
                    INSERT INTO trades (txid, pair, type, ordertype, price, vol, cost, fee, time, misc, raw_json, inserted_utc)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    row,
                )
                inserted += 1
            except sqlite3.IntegrityError:
                cur.execute(
                    """
                    UPDATE trades
                      SET pair=?, type=?, ordertype=?, price=?, vol=?, cost=?, fee=?, time=?, misc=?, raw_json=?
                    WHERE txid=?
                    """,
                    (
                        row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[0]
                    ),
                )
                updated += 1
        conn.commit()

    return (inserted, updated)


def list_trades(*, since: Optional[float] = None, pair: Optional[str] = None, limit: int = 500) -> List[Dict[str, Any]]:
    init_db()
    q = "SELECT txid, pair, type, ordertype, price, vol, cost, fee, time, misc FROM trades"
    args: List[Any] = []
    wh: List[str] = []
    if since is not None:
        wh.append("time >= ?")
        args.append(float(since))
    if pair:
        wh.append("pair = ?")
        args.append(pair)
    if wh:
        q += " WHERE " + " AND ".join(wh)
    q += " ORDER BY time DESC LIMIT ?"
    args.append(int(limit))
    with _connect() as conn:
        rows = conn.execute(q, args).fetchall()
    return [dict(r) for r in rows]


def performance_summary(*, since: Optional[float] = None) -> Dict[str, Any]:
    """Basic realized cashflow + fees from recorded trades (USD-quote assumption)."""
    init_db()
    wh = ""
    args: List[Any] = []
    if since is not None:
        wh = "WHERE time >= ?"
        args.append(float(since))
    with _connect() as conn:
        rows = conn.execute(f"SELECT pair, type, cost, fee FROM trades {wh}", args).fetchall()

    per_pair: Dict[str, Dict[str, float]] = {}
    total_cashflow = 0.0
    total_fees = 0.0
    for r in rows:
        pair = r[0] or "UNKNOWN"
        typ = (r[1] or "").lower()
        cost = float(r[2] or 0.0)
        fee = float(r[3] or 0.0)
        if pair not in per_pair:
            per_pair[pair] = {"cashflow_usd": 0.0, "fees_usd": 0.0, "trades": 0.0}
        if typ == "buy":
            cash = -(cost + fee)
        elif typ == "sell":
            cash = (cost - fee)
        else:
            cash = 0.0
        per_pair[pair]["cashflow_usd"] += cash
        per_pair[pair]["fees_usd"] += fee
        per_pair[pair]["trades"] += 1
        total_cashflow += cash
        total_fees += fee

    return {
        "ok": True,
        "db_path": _db_path(),
        "since": since,
        "total_cashflow_usd": total_cashflow,
        "total_fees_usd": total_fees,
        "per_pair": per_pair,
    }


def _to_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        return float(x)
    except Exception:
        return None
