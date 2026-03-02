import os, sqlite3, time, math
from typing import Any, Dict, List, Optional, Tuple

DEFAULT_DB_PATH = "/var/data/trades.sqlite3"

SCHEMA = """
CREATE TABLE IF NOT EXISTS trades (
    txid TEXT PRIMARY KEY,
    pair TEXT,
    side TEXT,
    ordertype TEXT,
    price REAL,
    vol REAL,
    cost REAL,
    fee REAL,
    time REAL
);

CREATE INDEX IF NOT EXISTS idx_trades_time ON trades(time);
CREATE INDEX IF NOT EXISTS idx_trades_pair_time ON trades(pair, time);
"""

def _db_path() -> str:
    return os.getenv("TRADES_DB_PATH", DEFAULT_DB_PATH)

def ensure_db() -> str:
    path = _db_path()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    con = sqlite3.connect(path)
    try:
        con.executescript(SCHEMA)
        con.commit()
    finally:
        con.close()
    return path

def upsert_trades(trades: List[Dict[str, Any]]) -> Dict[str, Any]:
    path = ensure_db()
    con = sqlite3.connect(path)
    con.row_factory = sqlite3.Row
    inserted = 0
    updated = 0
    try:
        cur = con.cursor()
        for t in trades:
            txid = t.get("txid")
            if not txid:
                continue
            # Normalize
            pair = t.get("pair")
            side = t.get("type") or t.get("side")
            ordertype = t.get("ordertype")
            price = float(t.get("price") or 0)
            vol = float(t.get("vol") or 0)
            cost = float(t.get("cost") or (price * vol))
            fee = float(t.get("fee") or 0)
            ts = float(t.get("time") or 0)
            # Upsert
            cur.execute(
                """
                INSERT INTO trades(txid,pair,side,ordertype,price,vol,cost,fee,time)
                VALUES(?,?,?,?,?,?,?,?,?)
                ON CONFLICT(txid) DO UPDATE SET
                    pair=excluded.pair,
                    side=excluded.side,
                    ordertype=excluded.ordertype,
                    price=excluded.price,
                    vol=excluded.vol,
                    cost=excluded.cost,
                    fee=excluded.fee,
                    time=excluded.time
                """,
                (txid, pair, side, ordertype, price, vol, cost, fee, ts),
            )
            if cur.rowcount == 1:
                inserted += 1
            else:
                updated += 1
        con.commit()
    finally:
        con.close()
    return {"ok": True, "db_path": path, "inserted": inserted, "updated": updated, "received": len(trades)}

def query_trades(since_hours: float = 24.0, pair: Optional[str] = None, limit: int = 200) -> Dict[str, Any]:
    path = ensure_db()
    since_ts = time.time() - float(since_hours) * 3600.0
    limit_i = max(1, min(int(limit), 5000))
    con = sqlite3.connect(path)
    con.row_factory = sqlite3.Row
    try:
        cur = con.cursor()
        if pair:
            cur.execute(
                """SELECT * FROM trades WHERE time >= ? AND pair = ? ORDER BY time DESC LIMIT ?""",
                (since_ts, pair, limit_i),
            )
        else:
            cur.execute(
                """SELECT * FROM trades WHERE time >= ? ORDER BY time DESC LIMIT ?""",
                (since_ts, limit_i),
            )
        rows = [dict(r) for r in cur.fetchall()]
    finally:
        con.close()
    return {"ok": True, "db_path": path, "time": time.time(), "since_hours": float(since_hours), "pair": pair, "limit": limit_i, "trades": rows}

def summary(days: float = 7.0) -> Dict[str, Any]:
    path = ensure_db()
    since_ts = time.time() - float(days) * 86400.0
    con = sqlite3.connect(path)
    con.row_factory = sqlite3.Row
    try:
        cur = con.cursor()
        # We don't know if trade is buy/sell; treat side == 'buy' as cash outflow, 'sell' as inflow
        cur.execute(
            """
            SELECT pair,
                   SUM(CASE WHEN lower(side)='sell' THEN cost ELSE 0 END) AS sells_cost,
                   SUM(CASE WHEN lower(side)='buy' THEN cost ELSE 0 END) AS buys_cost,
                   SUM(fee) AS fees
            FROM trades
            WHERE time >= ?
            GROUP BY pair
            """,
            (since_ts,),
        )
        per_pair = {}
        total_cashflow = 0.0
        total_fees = 0.0
        for r in cur.fetchall():
            pair = r["pair"] or "UNKNOWN"
            sells = float(r["sells_cost"] or 0)
            buys = float(r["buys_cost"] or 0)
            fees = float(r["fees"] or 0)
            cashflow = sells - buys - fees
            per_pair[pair] = {
                "sells_cost_usd": sells,
                "buys_cost_usd": buys,
                "fees_usd": fees,
                "net_cashflow_usd": cashflow,
            }
            total_cashflow += cashflow
            total_fees += fees
    finally:
        con.close()
    return {
        "ok": True,
        "db_path": path,
        "time": time.time(),
        "days": float(days),
        "since": since_ts,
        "total_cashflow_usd": total_cashflow,
        "total_fees_usd": total_fees,
        "per_pair": per_pair,
    }
