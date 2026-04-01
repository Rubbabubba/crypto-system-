from __future__ import annotations

import json
import os
import sqlite3
import time
from typing import Any, Dict, List, Optional


def _lifecycle_db_path() -> str:
    return os.getenv("LIFECYCLE_DB_PATH", "/var/data/lifecycle.sqlite3")


def _connect_lifecycle() -> sqlite3.Connection:
    path = _lifecycle_db_path()
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn

def _db_path() -> str:
    return os.getenv("TRADE_JOURNAL_DB_PATH", "/var/data/trade_journal.sqlite3")


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
            CREATE TABLE IF NOT EXISTS open_trades (
              symbol TEXT PRIMARY KEY,
              opened_ts REAL,
              strategy TEXT,
              source TEXT,
              signal_name TEXT,
              signal_id TEXT,
              req_id TEXT,
              entry_txid TEXT,
              entry_execution TEXT,
              entry_price REAL,
              entry_qty REAL,
              entry_cost REAL,
              entry_fee REAL,
              requested_notional_usd REAL,
              stop_price REAL,
              take_price REAL,
              meta_json TEXT,
              updated_utc REAL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS closed_trades (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              symbol TEXT,
              opened_ts REAL,
              closed_ts REAL,
              hold_sec REAL,
              strategy TEXT,
              source TEXT,
              signal_name TEXT,
              signal_id TEXT,
              req_id TEXT,
              entry_txid TEXT,
              exit_txid TEXT,
              entry_execution TEXT,
              exit_execution TEXT,
              entry_price REAL,
              exit_price REAL,
              entry_qty REAL,
              exit_qty REAL,
              entry_cost REAL,
              exit_cost REAL,
              entry_fee REAL,
              exit_fee REAL,
              fees_total REAL,
              gross_pnl_usd REAL,
              net_pnl_usd REAL,
              exit_reason TEXT,
              meta_json TEXT,
              created_utc REAL
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_closed_trades_closed_ts ON closed_trades(closed_ts)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_closed_trades_symbol_ts ON closed_trades(symbol, closed_ts)")
        conn.commit()


def _rowdict(row: sqlite3.Row | None) -> Optional[Dict[str, Any]]:
    return dict(row) if row is not None else None

def _lifecycle_strategy_from_intents(symbol: str, entry_txid: str = "", exit_txid: str = "") -> str:
    sym = str(symbol or "").strip().upper()
    try:
        with _connect_lifecycle() as lconn:
            for table_name, tx_col in (("trade_intents", "entry_txid"), ("entry_intents", "entry_txid")):
                try:
                    if entry_txid:
                        row = lconn.execute(
                            f"SELECT strategy FROM {table_name} WHERE symbol=? AND COALESCE({tx_col},'')=? AND COALESCE(strategy,'') <> '' ORDER BY id DESC LIMIT 1",
                            (sym, entry_txid),
                        ).fetchone()
                        if row is not None:
                            return str(row[0] or "").strip()
                except Exception:
                    pass
            for table_name in ("trade_intents", "entry_intents"):
                try:
                    row = lconn.execute(
                        f"SELECT strategy FROM {table_name} WHERE symbol=? AND COALESCE(strategy,'') <> '' ORDER BY id DESC LIMIT 1",
                        (sym,),
                    ).fetchone()
                    if row is not None:
                        return str(row[0] or "").strip()
                except Exception:
                    pass
    except Exception:
        return ""
    return ""


def _find_nearest_prior_buy(symbol: str, exit_txid: str, closed_ts: float, sell_qty: float) -> Optional[Dict[str, Any]]:
    sym = str(symbol or "").strip().upper()
    if not sym or not exit_txid:
        return None
    try:
        from crypto_light import broker_kraken  # type: ignore
        fills = broker_kraken.fetch_recent_fills(lookback_sec=7*86400) or []
    except Exception:
        return None
    candidates = []
    for f in fills:
        try:
            fsym = str(f.get("symbol") or "").strip().upper()
            side = str(f.get("side") or "").strip().lower()
            txid = str(f.get("txid") or "").strip()
            ts = _to_float(f.get("ts")) or 0.0
            qty = _to_float(f.get("qty")) or 0.0
            if fsym != sym or side != "buy" or txid == exit_txid:
                continue
            if ts <= 0.0 or ts > float(closed_ts):
                continue
            if sell_qty > 0 and qty > 0 and abs(qty - sell_qty) / max(sell_qty, 1e-9) > 0.10:
                continue
            candidates.append({
                "txid": txid,
                "qty": qty,
                "price": _to_float(f.get("price")) or 0.0,
                "cost": _to_float(f.get("cost")) or 0.0,
                "fee": _to_float(f.get("fee")) or 0.0,
                "ts": ts,
            })
        except Exception:
            continue
    if not candidates:
        return None
    candidates.sort(key=lambda x: abs(float(closed_ts) - float(x.get("ts") or 0.0)))
    return candidates[0]

def _lifecycle_strategy_from_trade_plans(symbol: str, entry_txid: str = "", plan_id: str = "") -> str:
    sym = str(symbol or "").strip().upper()
    try:
        with _connect_lifecycle() as lconn:
            if entry_txid:
                row = lconn.execute(
                    "SELECT strategy FROM trade_plans WHERE symbol=? AND (entry_txid=? OR plan_id=?) AND COALESCE(strategy,'') <> '' ORDER BY id DESC LIMIT 1",
                    (sym, entry_txid, plan_id or entry_txid),
                ).fetchone()
                if row is not None:
                    return str(row[0] or "").strip()
            row = lconn.execute(
                "SELECT strategy FROM trade_plans WHERE symbol=? AND COALESCE(strategy,'') <> '' ORDER BY id DESC LIMIT 1",
                (sym,),
            ).fetchone()
            if row is not None:
                return str(row[0] or "").strip()
    except Exception:
        return ""
    return ""

def _resolve_strategy_provenance(conn: sqlite3.Connection, symbol: str, open_row: Optional[Dict[str, Any]], exit_data: Dict[str, Any]) -> str:
    sym = str(symbol or "").strip().upper()
    if open_row:
        st = str(open_row.get("strategy") or "").strip()
        if st and st.lower() != "adopted":
            return st
    meta = exit_data.get("meta") or {}
    if isinstance(meta, dict):
        for key in ("strategy", "source_strategy", "planned_strategy", "entry_strategy"):
            st = str(meta.get(key) or "").strip()
            if st and st.lower() != "adopted":
                return st
    entry_txid = str(exit_data.get("entry_txid") or "").strip()
    exit_txid = str(exit_data.get("exit_txid") or "").strip()
    if entry_txid:
        row = conn.execute(
            "SELECT strategy FROM closed_trades WHERE symbol=? AND entry_txid=? AND COALESCE(strategy,'') <> '' ORDER BY id DESC LIMIT 1",
            (sym, entry_txid),
        ).fetchone()
        if row is not None:
            st = str(row[0] or "").strip()
            if st and st.lower() != "adopted":
                return st
    if exit_txid:
        row = conn.execute(
            "SELECT strategy FROM closed_trades WHERE symbol=? AND exit_txid=? AND COALESCE(strategy,'') <> '' ORDER BY id DESC LIMIT 1",
            (sym, exit_txid),
        ).fetchone()
        if row is not None:
            st = str(row[0] or "").strip()
            if st and st.lower() != "adopted":
                return st
    st = _lifecycle_strategy_from_trade_plans(sym, entry_txid=entry_txid, plan_id=entry_txid)
    if st and st.lower() != "adopted":
        return st
    st = _lifecycle_strategy_from_intents(sym, entry_txid=entry_txid, exit_txid=exit_txid)
    if st and st.lower() != "adopted":
        return st
    if open_row:
        st = str(open_row.get("strategy") or "").strip()
        if st:
            return st
    return "adopted"




def _lifecycle_table_columns(conn: sqlite3.Connection, table_name: str) -> List[str]:
    try:
        rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        return [str(r[1]) for r in rows if len(r) > 1]
    except Exception:
        return []

def _pick_first(cols: List[str], candidates: List[str]) -> str:
    lower = {c.lower(): c for c in cols}
    for cand in candidates:
        if cand.lower() in lower:
            return lower[cand.lower()]
    return ""

def _fetch_bridge_candidates_from_table(conn: sqlite3.Connection, table_name: str, symbol: str, entry_txid: str = "", exit_txid: str = "", opened_ts: float = 0.0, closed_ts: float = 0.0, limit: int = 20) -> List[Dict[str, Any]]:
    cols = _lifecycle_table_columns(conn, table_name)
    if not cols:
        return []
    sym_col = _pick_first(cols, ["symbol", "pair", "market"])
    strat_col = _pick_first(cols, ["strategy", "entry_strategy", "signal_strategy", "planned_strategy"])
    created_col = _pick_first(cols, ["created_ts", "created_at_ts", "opened_ts", "entry_ts", "ts", "created_at"])
    entry_col = _pick_first(cols, ["entry_txid", "buy_txid", "open_txid", "txid", "entry_order_txid"])
    exit_col = _pick_first(cols, ["exit_txid", "sell_txid", "close_txid", "exit_order_txid"])
    if not sym_col or not strat_col:
        return []
    where = [f"COALESCE({sym_col},'') = ?"]
    params = [symbol]
    tx_parts = []
    if entry_txid and entry_col:
        tx_parts.append(f"COALESCE({entry_col},'') = ?")
        params.append(entry_txid)
    if exit_txid and exit_col:
        tx_parts.append(f"COALESCE({exit_col},'') = ?")
        params.append(exit_txid)
    target_ts = float(opened_ts or closed_ts or 0.0)
    sql = f"SELECT * FROM {table_name}"
    if tx_parts:
        sql += " WHERE " + " AND ".join(where) + " AND (" + " OR ".join(tx_parts) + ")"
    else:
        sql += " WHERE " + " AND ".join(where)
    order_parts = []
    if created_col and target_ts > 0:
        order_parts.append(f"ABS(COALESCE({created_col},0)-{target_ts}) ASC")
    order_parts.append("rowid DESC")
    sql += " ORDER BY " + ", ".join(order_parts) + f" LIMIT {int(limit)}"
    try:
        return [dict(r) for r in conn.execute(sql, tuple(params)).fetchall()]
    except Exception:
        return []

def _extract_strategy_from_bridge_row(row: Dict[str, Any]) -> str:
    for key in ("strategy", "entry_strategy", "signal_strategy", "planned_strategy"):
        val = str(row.get(key) or "").strip()
        if val and val.lower() != "adopted":
            return val
    return ""


def _fetch_family_rows_from_table(conn: sqlite3.Connection, table_name: str, symbol: str, target_ts: float, entry_txid: str = "", exit_txid: str = "", limit: int = 50) -> List[Dict[str, Any]]:
    cols = _lifecycle_table_columns(conn, table_name)
    if not cols:
        return []
    sym_col = _pick_first(cols, ["symbol", "pair", "market"])
    strat_col = _pick_first(cols, ["strategy", "entry_strategy", "signal_strategy", "planned_strategy"])
    if not sym_col or not strat_col:
        return []
    created_col = _pick_first(cols, ["created_ts", "created_at_ts", "opened_ts", "entry_ts", "closed_ts", "ts", "created_at"])
    entry_col = _pick_first(cols, ["entry_txid", "buy_txid", "open_txid", "txid", "entry_order_txid"])
    exit_col = _pick_first(cols, ["exit_txid", "sell_txid", "close_txid", "exit_order_txid"])
    where = [f"COALESCE({sym_col},'')=?"]
    params = [symbol]
    tx_clauses = []
    if entry_txid and entry_col:
        tx_clauses.append(f"COALESCE({entry_col},'')=?")
        params.append(entry_txid)
    if exit_txid and exit_col:
        tx_clauses.append(f"COALESCE({exit_col},'')=?")
        params.append(exit_txid)
    sql = f"SELECT * FROM {table_name} WHERE " + " AND ".join(where)
    if tx_clauses:
        sql += " AND (" + " OR ".join(tx_clauses) + ")"
    order_parts = []
    if created_col and target_ts > 0:
        order_parts.append(f"ABS(COALESCE({created_col},0)-{target_ts}) ASC")
    order_parts.append("rowid DESC")
    sql += " ORDER BY " + ", ".join(order_parts) + f" LIMIT {int(limit)}"
    try:
        return [dict(r) for r in conn.execute(sql, tuple(params)).fetchall()]
    except Exception:
        return []

def _score_family_candidate(row: Dict[str, Any], target_ts: float, entry_txid: str = "", exit_txid: str = "") -> float:
    score = 0.0
    strat = _extract_strategy_from_bridge_row(row)
    if strat and strat.lower() != "adopted":
        score += 100.0
    joined = " ".join([str(v or "") for v in row.values()]).lower()
    if entry_txid and entry_txid.lower() in joined:
        score += 40.0
    if exit_txid and exit_txid.lower() in joined:
        score += 40.0
    closest = None
    for k in ("created_ts", "created_at_ts", "opened_ts", "entry_ts", "closed_ts", "ts"):
        try:
            if k in row and row.get(k) is not None:
                dist = abs(float(row.get(k) or 0.0) - float(target_ts or 0.0))
                closest = dist if closest is None else min(closest, dist)
        except Exception:
            pass
    if closest is not None:
        if closest <= 120:
            score += 25.0
        elif closest <= 600:
            score += 18.0
        elif closest <= 1800:
            score += 10.0
        elif closest <= 7200:
            score += 5.0
    return score

def _family_specific_backfill_strategy_map(symbol: str, entry_txid: str = "", exit_txid: str = "", opened_ts: float = 0.0, closed_ts: float = 0.0, exit_reason: str = "") -> Dict[str, Any]:
    sym = str(symbol or "").strip().upper()
    target_ts = float(closed_ts or opened_ts or 0.0)
    if str(exit_reason or "").startswith("reconciled_fill_backfill"):
        families = ["trade_lifecycle_events", "trade_plans", "trade_intents", "entry_intents"]
    elif str(exit_reason or "").startswith("reconciled_fill"):
        families = ["trade_plans", "trade_lifecycle_events", "trade_intents", "entry_intents"]
    else:
        families = ["trade_plans", "trade_intents", "entry_intents", "trade_lifecycle_events"]
    best = {"strategy": "", "source": "", "row": {}, "score": -1.0}
    try:
        with _connect_lifecycle() as lconn:
            for table_name in families:
                rows = _fetch_family_rows_from_table(lconn, table_name, sym, target_ts=target_ts, entry_txid=entry_txid, exit_txid=exit_txid, limit=50)
                for row in rows:
                    strat = _extract_strategy_from_bridge_row(row)
                    if not strat or strat.lower() == "adopted":
                        continue
                    score = _score_family_candidate(row, target_ts, entry_txid=entry_txid, exit_txid=exit_txid)
                    if score > float(best["score"]):
                        best = {"strategy": strat, "source": f"{table_name}:family_map", "row": row, "score": score}
    except Exception:
        pass
    return best

def _explicit_lifecycle_provenance_bridge(symbol: str, entry_txid: str = "", exit_txid: str = "", opened_ts: float = 0.0, closed_ts: float = 0.0) -> Dict[str, Any]:
    sym = str(symbol or "").strip().upper()
    if not sym:
        return {"strategy": "", "source": "", "row": {}}
    lifecycle_tables = ["trade_plans", "trade_intents", "entry_intents", "trade_lifecycle_events", "trade_workflow_locks"]
    try:
        with _connect_lifecycle() as lconn:
            for table_name in lifecycle_tables:
                rows = _fetch_bridge_candidates_from_table(lconn, table_name, sym, entry_txid=entry_txid, exit_txid=exit_txid, opened_ts=opened_ts, closed_ts=closed_ts, limit=20)
                for row in rows:
                    st = _extract_strategy_from_bridge_row(row)
                    if st:
                        return {"strategy": st, "source": table_name + ":txid_bridge", "row": row}
            for table_name in lifecycle_tables:
                rows = _fetch_bridge_candidates_from_table(lconn, table_name, sym, opened_ts=opened_ts, closed_ts=closed_ts, limit=20)
                for row in rows:
                    st = _extract_strategy_from_bridge_row(row)
                    if st:
                        return {"strategy": st, "source": table_name + ":time_bridge", "row": row}
    except Exception:
        pass
    return {"strategy": "", "source": "", "row": {}}

def _resolve_journal_strategy_from_lifecycle(symbol: str, entry_txid: str = "", exit_txid: str = "", opened_ts: float = 0.0, closed_ts: float = 0.0, exit_reason: str = "") -> str:
    sym = str(symbol or "").strip().upper()
    bridge = _explicit_lifecycle_provenance_bridge(sym, entry_txid=entry_txid, exit_txid=exit_txid, opened_ts=opened_ts, closed_ts=closed_ts, exit_reason=exit_reason)
    st = str((bridge or {}).get("strategy") or "").strip()
    if st and st.lower() != "adopted":
        return st
    st = _lifecycle_strategy_from_trade_plans(sym, entry_txid=entry_txid, plan_id=entry_txid)
    if st and st.lower() != "adopted":
        return st
    st = _lifecycle_strategy_from_intents(sym, entry_txid=entry_txid, exit_txid=exit_txid)
    if st and st.lower() != "adopted":
        return st
    # time-adjacent fallback from lifecycle trade_plans/intents by symbol
    try:
        with _connect_lifecycle() as lconn:
            for table_name, ts_col in (("trade_plans", "created_ts"), ("trade_intents", "created_ts"), ("entry_intents", "created_ts")):
                try:
                    target_ts = float(opened_ts or closed_ts or 0.0)
                    rows = lconn.execute(
                        f"SELECT strategy, COALESCE({ts_col},0) AS ev_ts FROM {table_name} WHERE symbol=? AND COALESCE(strategy,'') <> '' ORDER BY ABS(COALESCE({ts_col},0)-?) ASC, id DESC LIMIT 5",
                        (sym, target_ts),
                    ).fetchall()
                    for row in rows:
                        cand = str(row[0] or "").strip()
                        if cand and cand.lower() != "adopted":
                            return cand
                except Exception:
                    pass
    except Exception:
        pass
    return "adopted"



def force_rewrite_adopted_journal_strategies(*, lookback_days: float = 30.0) -> Dict[str, Any]:
    init_db()
    now = time.time()
    since_ts = max(0.0, float(now) - float(lookback_days) * 86400.0)
    updated = []
    checked = 0
    with _connect() as conn:
        rows = conn.execute("SELECT id, symbol, strategy, entry_txid, exit_txid, entry_ts, closed_ts FROM closed_trades WHERE closed_ts >= ? ORDER BY id DESC", (since_ts,)).fetchall()
        for r in rows:
            row = dict(r)
            checked += 1
            current = str(row.get("strategy") or "").strip().lower()
            if current and current != "adopted":
                continue
            bridge = _explicit_lifecycle_provenance_bridge(
                str(row.get("symbol") or ""),
                entry_txid=str(row.get("entry_txid") or ""),
                exit_txid=str(row.get("exit_txid") or ""),
                opened_ts=_to_float(row.get("entry_ts")) or 0.0,
                closed_ts=_to_float(row.get("closed_ts")) or 0.0,
                exit_reason=str(row.get("exit_reason") or ""),
            )
            new_strategy = str((bridge or {}).get("strategy") or "").strip()
            if new_strategy and new_strategy.lower() != "adopted":
                conn.execute("UPDATE closed_trades SET strategy=? WHERE id=?", (new_strategy, int(row["id"])))
                updated.append({"table": "closed_trades", "id": int(row["id"]), "symbol": row.get("symbol"), "to": new_strategy, "source": bridge.get("source")})
        rows = conn.execute("SELECT id, symbol, strategy, entry_txid, opened_ts FROM open_trades ORDER BY id DESC").fetchall()
        for r in rows:
            row = dict(r)
            checked += 1
            current = str(row.get("strategy") or "").strip().lower()
            if current and current != "adopted":
                continue
            bridge = _explicit_lifecycle_provenance_bridge(
                str(row.get("symbol") or ""),
                entry_txid=str(row.get("entry_txid") or ""),
                opened_ts=_to_float(row.get("opened_ts")) or 0.0,
            )
            new_strategy = str((bridge or {}).get("strategy") or "").strip()
            if new_strategy and new_strategy.lower() != "adopted":
                conn.execute("UPDATE open_trades SET strategy=? WHERE id=?", (new_strategy, int(row["id"])))
                updated.append({"table": "open_trades", "id": int(row["id"]), "symbol": row.get("symbol"), "to": new_strategy, "source": bridge.get("source")})
        conn.commit()
    return {"ok": True, "checked": checked, "updated_count": len(updated), "updated_rows": updated[:100]}

def rewrite_journal_strategies_from_lifecycle(*, lookback_days: float = 30.0) -> Dict[str, Any]:
    init_db()
    now = time.time()
    since_ts = max(0.0, float(now) - float(lookback_days) * 86400.0)
    updated = []
    checked = 0
    with _connect() as conn:
        # closed trades
        rows = conn.execute(
            "SELECT id, symbol, strategy, entry_txid, exit_txid, entry_ts, closed_ts FROM closed_trades WHERE closed_ts >= ? ORDER BY id DESC",
            (since_ts,),
        ).fetchall()
        for r in rows:
            row = dict(r)
            checked += 1
            current = str(row.get("strategy") or "").strip()
            if current and current.lower() != "adopted":
                continue
            resolved = _resolve_journal_strategy_from_lifecycle(
                row.get("symbol") or "",
                entry_txid=str(row.get("entry_txid") or ""),
                exit_txid=str(row.get("exit_txid") or ""),
                opened_ts=_to_float(row.get("entry_ts")) or 0.0,
                closed_ts=_to_float(row.get("closed_ts")) or 0.0,
            )
            if resolved and resolved.lower() != "adopted":
                conn.execute("UPDATE closed_trades SET strategy=? WHERE id=?", (resolved, int(row["id"])))
                updated.append({"table": "closed_trades", "id": int(row["id"]), "symbol": row.get("symbol"), "to": resolved})
        # open trades
        rows = conn.execute(
            "SELECT id, symbol, strategy, entry_txid, opened_ts FROM open_trades ORDER BY id DESC"
        ).fetchall()
        for r in rows:
            row = dict(r)
            checked += 1
            current = str(row.get("strategy") or "").strip()
            if current and current.lower() != "adopted":
                continue
            resolved = _resolve_journal_strategy_from_lifecycle(
                row.get("symbol") or "",
                entry_txid=str(row.get("entry_txid") or ""),
                opened_ts=_to_float(row.get("opened_ts")) or 0.0,
            )
            if resolved and resolved.lower() != "adopted":
                conn.execute("UPDATE open_trades SET strategy=? WHERE id=?", (resolved, int(row["id"])))
                updated.append({"table": "open_trades", "id": int(row["id"]), "symbol": row.get("symbol"), "to": resolved})
        conn.commit()
    return {"ok": True, "checked": checked, "updated_count": len(updated), "updated_rows": updated[:100]}


def rehydrate_unmatched_backfill_from_broker_history(*, lookback_days: float = 30.0) -> Dict[str, Any]:
    init_db()
    now = time.time()
    lookback_sec = float(lookback_days) * 86400.0
    repaired = []
    skipped = []
    try:
        from crypto_light import broker_kraken  # type: ignore
        fills = broker_kraken.fetch_recent_fills(lookback_sec=int(lookback_sec)) or []
    except Exception as e:
        return {"ok": False, "error": f"fetch_recent_fills_failed:{e}"}
    by_symbol = {}
    for f in fills:
        sym = str(f.get("symbol") or "").strip().upper()
        by_symbol.setdefault(sym, []).append(f)
    with _connect() as conn:
        for sym, fs in by_symbol.items():
            sells = [f for f in fs if str(f.get("side") or "").lower() == "sell"]
            buys = [f for f in fs if str(f.get("side") or "").lower() == "buy"]
            for sell in sells:
                sell_txid = str(sell.get("txid") or "").strip()
                if not sell_txid:
                    continue
                existing = conn.execute("SELECT id FROM closed_trades WHERE symbol=? AND exit_txid=? LIMIT 1", (sym, sell_txid)).fetchone()
                if existing is not None:
                    continue
                candidates = [b for b in buys if float(b.get("time") or 0.0) <= float(sell.get("time") or 0.0)]
                if not candidates:
                    recovered = _find_nearest_prior_buy(sym, sell_txid, float(sell.get("time") or 0.0), float(sell.get("qty") or 0.0))
                    if recovered:
                        buy = {"txid": recovered.get("txid"), "qty": recovered.get("qty"), "price": recovered.get("price"), "cost": recovered.get("cost"), "fee": recovered.get("fee"), "time": recovered.get("ts")}
                    else:
                        skipped.append({"symbol": sym, "txid": sell_txid, "reason": "no_prior_buy_match"})
                        continue
                else:
                    candidates.sort(key=lambda x: float(x.get("time") or 0.0))
                    buy = candidates[-1]
                entry_qty = min(float(sell.get("qty") or 0.0), float(buy.get("qty") or 0.0))
                if entry_qty <= 0:
                    skipped.append({"symbol": sym, "txid": sell_txid, "reason": "invalid_qty_match"})
                    continue
                strategy = _resolve_journal_strategy_from_lifecycle(sym, entry_txid=str(buy.get("txid") or ""), exit_txid=sell_txid, opened_ts=float(buy.get("time") or 0.0), closed_ts=float(sell.get("time") or 0.0))
                open_payload = {
                    "symbol": sym,
                    "opened_ts": float(buy.get("time") or now),
                    "strategy": strategy or "adopted",
                    "source": "broker_trade_history_rehydration",
                    "entry_txid": str(buy.get("txid") or ""),
                    "entry_price": float(buy.get("price") or 0.0),
                    "entry_qty": float(entry_qty),
                    "entry_cost": float(buy.get("cost") or 0.0) * (entry_qty / max(float(buy.get("qty") or entry_qty), 1e-9)),
                    "entry_fee": float(buy.get("fee") or 0.0) * (entry_qty / max(float(buy.get("qty") or entry_qty), 1e-9)),
                    "requested_notional_usd": float(buy.get("cost") or 0.0) * (entry_qty / max(float(buy.get("qty") or entry_qty), 1e-9)),
                    "meta": {"rehydrated": True, "sell_txid": sell_txid},
                }
                try:
                    conn.execute("DELETE FROM open_trades WHERE symbol=?", (sym,))
                    conn.commit()
                except Exception:
                    pass
                opened = open_trade(open_payload)
                if not opened or not opened.get("ok"):
                    skipped.append({"symbol": sym, "txid": sell_txid, "reason": "open_trade_failed", "result": opened})
                    continue
                closed = close_trade(sym, {
                    "closed_ts": float(sell.get("time") or now),
                    "exit_txid": sell_txid,
                    "exit_price": float(sell.get("price") or 0.0),
                    "exit_qty": float(entry_qty),
                    "exit_cost": float(sell.get("cost") or 0.0) * (entry_qty / max(float(sell.get("qty") or entry_qty), 1e-9)),
                    "exit_fee": float(sell.get("fee") or 0.0) * (entry_qty / max(float(sell.get("qty") or entry_qty), 1e-9)),
                    "exit_reason": "reconciled_fill_rehydrated",
                    "meta": {"rehydrated": True, "matched_buy_txid": str(buy.get("txid") or "")},
                })
                if closed and closed.get("ok"):
                    repaired.append({"symbol": sym, "entry_txid": str(buy.get("txid") or ""), "exit_txid": sell_txid, "strategy": strategy or "adopted"})
                else:
                    skipped.append({"symbol": sym, "txid": sell_txid, "reason": "close_trade_failed", "result": closed})
        try:
            from crypto_light import telemetry_db  # type: ignore
            telemetry_db.sync_from_trade_journal(limit=500)
        except Exception:
            pass
    return {"ok": True, "rehydrated_count": len(repaired), "rehydrated_rows": repaired[:100], "skipped": skipped[:100]}
def repair_reconciled_strategy_attribution(*, lookback_days: float = 30.0) -> Dict[str, Any]:
    init_db()
    now = time.time()
    since_ts = max(0.0, float(now) - float(lookback_days) * 86400.0)
    updated = []
    checked = 0
    with _connect() as conn:
        rows = conn.execute(
            "SELECT * FROM closed_trades WHERE closed_ts >= ? ORDER BY closed_ts DESC, id DESC",
            (since_ts,),
        ).fetchall()
        for r in rows:
            checked += 1
            row = dict(r)
            current = str(row.get("strategy") or "").strip()
            if current and current.lower() != "adopted":
                continue
            exit_reason = str(row.get("exit_reason") or "")
            if not (exit_reason.startswith("reconciled_fill") or exit_reason == "reconciled_fill_backfill" or current.lower() == "adopted"):
                continue
            sym = str(row.get("symbol") or "").strip().upper()
            entry_txid = str(row.get("entry_txid") or "").strip()
            exit_txid = str(row.get("exit_txid") or "").strip()
            candidate = ""
            if entry_txid:
                o = conn.execute(
                    "SELECT strategy FROM open_trades WHERE symbol=? AND entry_txid=? AND COALESCE(strategy,'') <> '' ORDER BY id DESC LIMIT 1",
                    (sym, entry_txid),
                ).fetchone()
                if o is not None:
                    candidate = str(o[0] or "").strip()
            if (not candidate or candidate.lower() == "adopted") and entry_txid:
                q = conn.execute(
                    "SELECT strategy FROM closed_trades WHERE symbol=? AND entry_txid=? AND id <> ? AND COALESCE(strategy,'') <> '' ORDER BY id DESC LIMIT 1",
                    (sym, entry_txid, int(row.get("id") or 0)),
                ).fetchone()
                if q is not None:
                    candidate = str(q[0] or "").strip()
            if (not candidate or candidate.lower() == "adopted") and exit_txid:
                q = conn.execute(
                    "SELECT strategy FROM closed_trades WHERE symbol=? AND exit_txid=? AND id <> ? AND COALESCE(strategy,'') <> '' ORDER BY id DESC LIMIT 1",
                    (sym, exit_txid, int(row.get("id") or 0)),
                ).fetchone()
                if q is not None:
                    candidate = str(q[0] or "").strip()
            if not candidate or candidate.lower() == "adopted":
                candidate = _lifecycle_strategy_from_trade_plans(sym, entry_txid=entry_txid, plan_id=entry_txid)
            if not candidate or candidate.lower() == "adopted":
                candidate = _lifecycle_strategy_from_intents(sym, entry_txid=entry_txid, exit_txid=exit_txid)
            if candidate and candidate.lower() != "adopted":
                conn.execute("UPDATE closed_trades SET strategy=? WHERE id=?", (candidate, int(row.get("id") or 0)))
                updated.append({"id": int(row.get("id") or 0), "symbol": sym, "from": current or "adopted", "to": candidate, "exit_txid": exit_txid})
        conn.commit()
    return {"ok": True, "checked": checked, "updated_count": len(updated), "updated_rows": updated[:50]}

def _find_closed_by_symbol_exit_txid(conn: sqlite3.Connection, symbol: str, exit_txid: str) -> Optional[Dict[str, Any]]:
    sym = str(symbol or "").strip().upper()
    tx = str(exit_txid or "").strip()
    if not sym or not tx:
        return None
    row = conn.execute(
        "SELECT * FROM closed_trades WHERE symbol=? AND exit_txid=? ORDER BY closed_ts DESC, id DESC LIMIT 1",
        (sym, tx),
    ).fetchone()
    return dict(row) if row is not None else None


def repair_reconciled_exit_truth(*, lookback_days: float = 30.0) -> Dict[str, Any]:
    init_db()
    now = time.time()
    since_ts = max(0.0, float(now) - float(lookback_days) * 86400.0)
    repaired_rows = []
    deduped_rows = []
    checked = 0
    with _connect() as conn:
        rows = conn.execute(
            "SELECT * FROM closed_trades WHERE closed_ts >= ? ORDER BY closed_ts DESC, id DESC",
            (since_ts,),
        ).fetchall()
        rows = [dict(r) for r in rows]
        seen_by_key = {}
        for r in rows:
            checked += 1
            rid = int(r.get("id") or 0)
            symbol = str(r.get("symbol") or "").strip().upper()
            exit_txid = str(r.get("exit_txid") or "").strip()
            exit_reason = str(r.get("exit_reason") or "")
            try:
                meta = json.loads(r.get("meta_json") or "{}")
            except Exception:
                meta = {}
            matched_fill = meta.get("matched_fill") if isinstance(meta, dict) else {}
            # 1) Recompute reconciled exit truth when full fill cost/fee was applied to a tiny residual quantity.
            if exit_reason.startswith("reconciled_fill") or bool((meta or {}).get("reconciled_close")):
                exit_qty = _to_float(r.get("exit_qty")) or 0.0
                exit_price = _to_float(r.get("exit_price")) or 0.0
                entry_cost = _to_float(r.get("entry_cost")) or 0.0
                entry_fee = _to_float(r.get("entry_fee")) or 0.0
                exit_fee = _to_float(r.get("exit_fee")) or 0.0
                exit_cost = _to_float(r.get("exit_cost")) or 0.0
                mf_qty = 0.0
                mf_cost = 0.0
                mf_fee = 0.0
                if isinstance(matched_fill, dict):
                    mf_qty = _to_float(matched_fill.get("qty")) or 0.0
                    mf_cost = _to_float(matched_fill.get("cost")) or 0.0
                    mf_fee = _to_float(matched_fill.get("fee")) or 0.0
                expected_cost = exit_price * exit_qty if exit_price > 0.0 and exit_qty > 0.0 else 0.0
                suspicious = False
                new_exit_cost = exit_cost
                new_exit_fee = exit_fee
                if mf_qty > 0.0 and exit_qty > 0.0 and exit_qty < (mf_qty * 0.999):
                    suspicious = True
                    scale = max(0.0, min(1.0, float(exit_qty) / float(mf_qty)))
                    if mf_cost > 0.0:
                        new_exit_cost = mf_cost * scale
                    elif expected_cost > 0.0:
                        new_exit_cost = expected_cost
                    if mf_fee > 0.0:
                        new_exit_fee = mf_fee * scale
                elif expected_cost > 0.0 and exit_cost > (expected_cost * 1.5):
                    suspicious = True
                    new_exit_cost = expected_cost
                if suspicious:
                    gross = float(new_exit_cost) - float(entry_cost)
                    net = float(gross) - float(entry_fee) - float(new_exit_fee)
                    fees_total = float(entry_fee) + float(new_exit_fee)
                    conn.execute(
                        "UPDATE closed_trades SET exit_cost=?, exit_fee=?, fees_total=?, gross_pnl_usd=?, net_pnl_usd=? WHERE id=?",
                        (_to_float(new_exit_cost), _to_float(new_exit_fee), _to_float(fees_total), _to_float(gross), _to_float(net), rid),
                    )
                    repaired_rows.append({
                        "id": rid,
                        "symbol": symbol,
                        "exit_txid": exit_txid,
                        "old_exit_cost": exit_cost,
                        "new_exit_cost": new_exit_cost,
                        "old_exit_fee": exit_fee,
                        "new_exit_fee": new_exit_fee,
                        "old_net_pnl_usd": _to_float(r.get("net_pnl_usd")),
                        "new_net_pnl_usd": net,
                    })
            # 2) Remove duplicate reconciled rows for the same broker exit txid and symbol, preserving the latest non-reconciled row when present.
            if symbol and exit_txid:
                key = (symbol, exit_txid)
                incumbent = seen_by_key.get(key)
                if incumbent is None:
                    seen_by_key[key] = r
                else:
                    incumbent_reason = str(incumbent.get("exit_reason") or "")
                    incumbent_id = int(incumbent.get("id") or 0)
                    current_is_reconciled = exit_reason.startswith("reconciled_fill") or bool((meta or {}).get("reconciled_close"))
                    incumbent_is_reconciled = incumbent_reason.startswith("reconciled_fill")
                    delete_id = None
                    if current_is_reconciled and not incumbent_is_reconciled:
                        delete_id = rid
                    elif incumbent_is_reconciled and not current_is_reconciled:
                        delete_id = incumbent_id
                        seen_by_key[key] = r
                    elif current_is_reconciled and incumbent_is_reconciled:
                        delete_id = rid if rid < incumbent_id else incumbent_id
                        if delete_id == incumbent_id:
                            seen_by_key[key] = r
                    if delete_id:
                        conn.execute("DELETE FROM closed_trades WHERE id=?", (int(delete_id),))
                        deduped_rows.append({"symbol": symbol, "exit_txid": exit_txid, "deleted_id": int(delete_id)})
        conn.commit()
    return {
        "ok": True,
        "checked": checked,
        "repaired_count": len(repaired_rows),
        "deduped_count": len(deduped_rows),
        "repaired_rows": repaired_rows[:50],
        "deduped_rows": deduped_rows[:50],
    }


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


def _closed_trade_fingerprint(row: Dict[str, Any]) -> str:
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


def _dedupe_closed_trade_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = set()
    out: List[Dict[str, Any]] = []
    for row in rows:
        fp = _closed_trade_fingerprint(row)
        if fp in seen:
            continue
        seen.add(fp)
        out.append(row)
    return out


def get_open_trade(symbol: str) -> Optional[Dict[str, Any]]:
    init_db()
    with _connect() as conn:
        row = conn.execute("SELECT * FROM open_trades WHERE symbol=?", (str(symbol),)).fetchone()
    return _rowdict(row)


def list_open_trades(limit: int = 200) -> List[Dict[str, Any]]:
    init_db()
    with _connect() as conn:
        rows = conn.execute(
            "SELECT * FROM open_trades ORDER BY opened_ts DESC LIMIT ?", (max(1, int(limit)),)
        ).fetchall()
    return _dedupe_closed_trade_rows([dict(r) for r in rows])


def delete_open_trade(symbol: str) -> Dict[str, Any]:
    init_db()
    sym = str(symbol or '').strip().upper()
    if not sym:
        raise ValueError('symbol is required')
    with _connect() as conn:
        conn.execute("DELETE FROM open_trades WHERE symbol=?", (sym,))
        conn.commit()
    return {"ok": True, "db_path": _db_path(), "symbol": sym}


def upsert_open_trade(trade: Dict[str, Any]) -> Dict[str, Any]:
    init_db()
    now = time.time()
    symbol = str(trade.get("symbol") or "").strip().upper()
    if not symbol:
        raise ValueError("symbol is required")
    payload = {
        "symbol": symbol,
        "opened_ts": float(trade.get("opened_ts") or now),
        "strategy": trade.get("strategy"),
        "source": trade.get("source"),
        "signal_name": trade.get("signal_name"),
        "signal_id": trade.get("signal_id"),
        "req_id": trade.get("req_id"),
        "entry_txid": trade.get("entry_txid"),
        "entry_execution": trade.get("entry_execution"),
        "entry_price": _to_float(trade.get("entry_price")),
        "entry_qty": _to_float(trade.get("entry_qty")),
        "entry_cost": _to_float(trade.get("entry_cost")),
        "entry_fee": _to_float(trade.get("entry_fee")),
        "requested_notional_usd": _to_float(trade.get("requested_notional_usd")),
        "stop_price": _to_float(trade.get("stop_price")),
        "take_price": _to_float(trade.get("take_price")),
        "meta_json": json.dumps(trade.get("meta") or {}, separators=(",", ":"), sort_keys=True),
        "updated_utc": now,
    }
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO open_trades (
              symbol, opened_ts, strategy, source, signal_name, signal_id, req_id,
              entry_txid, entry_execution, entry_price, entry_qty, entry_cost, entry_fee,
              requested_notional_usd, stop_price, take_price, meta_json, updated_utc
            ) VALUES (
              :symbol, :opened_ts, :strategy, :source, :signal_name, :signal_id, :req_id,
              :entry_txid, :entry_execution, :entry_price, :entry_qty, :entry_cost, :entry_fee,
              :requested_notional_usd, :stop_price, :take_price, :meta_json, :updated_utc
            )
            ON CONFLICT(symbol) DO UPDATE SET
              opened_ts=excluded.opened_ts,
              strategy=excluded.strategy,
              source=excluded.source,
              signal_name=excluded.signal_name,
              signal_id=excluded.signal_id,
              req_id=excluded.req_id,
              entry_txid=excluded.entry_txid,
              entry_execution=excluded.entry_execution,
              entry_price=excluded.entry_price,
              entry_qty=excluded.entry_qty,
              entry_cost=excluded.entry_cost,
              entry_fee=excluded.entry_fee,
              requested_notional_usd=excluded.requested_notional_usd,
              stop_price=excluded.stop_price,
              take_price=excluded.take_price,
              meta_json=excluded.meta_json,
              updated_utc=excluded.updated_utc
            """,
            payload,
        )
        conn.commit()
    return {"ok": True, "db_path": _db_path(), "symbol": symbol}


def close_trade(symbol: str, exit_data: Dict[str, Any]) -> Dict[str, Any]:
    init_db()
    sym = str(symbol or "").strip().upper()
    if not sym:
        raise ValueError("symbol is required")
    with _connect() as conn:
        open_row = conn.execute("SELECT * FROM open_trades WHERE symbol=?", (sym,)).fetchone()
        if open_row is None:
            return {"ok": False, "error": "open_trade_not_found", "symbol": sym}
        o = dict(open_row)
        now = time.time()
        closed_ts = _to_float(exit_data.get("closed_ts")) or now
        exit_txid = str(exit_data.get("exit_txid") or "").strip()
        meta = exit_data.get("meta") or {}
        meta = dict(meta)
        resolved_strategy = _resolve_strategy_provenance(conn, sym, o, exit_data)
        existing_closed = _find_closed_by_symbol_exit_txid(conn, sym, exit_txid) if exit_txid else None
        if existing_closed is not None and (bool(meta.get("reconciled_close")) or str(exit_data.get("exit_reason") or "").startswith("reconciled_fill")):
            try:
                conn.execute("DELETE FROM open_trades WHERE symbol=?", (sym,))
                conn.commit()
            except Exception:
                pass
            if str(existing_closed.get("strategy") or "").strip().lower() == "adopted" and resolved_strategy and resolved_strategy.lower() != "adopted":
                try:
                    conn.execute("UPDATE closed_trades SET strategy=? WHERE id=?", (resolved_strategy, int(existing_closed.get("id") or 0)))
                    conn.commit()
                    existing_closed["strategy"] = resolved_strategy
                except Exception:
                    pass
            existing_closed["ok"] = True
            existing_closed["deduped_reconciled_exit"] = True
            existing_closed["db_path"] = _db_path()
            existing_closed["remaining_qty"] = 0.0
            existing_closed["partial_close"] = False
            return existing_closed
        entry_qty_total = _to_float(o.get("entry_qty")) or 0.0
        exit_qty = _to_float(exit_data.get("exit_qty")) or 0.0
        if exit_qty <= 0.0:
            exit_qty = entry_qty_total
        exit_qty = max(0.0, min(float(exit_qty), float(entry_qty_total) if entry_qty_total > 0 else float(exit_qty)))
        entry_price = _to_float(o.get("entry_price")) or 0.0
        exit_price = _to_float(exit_data.get("exit_price")) or 0.0
        entry_cost_total = _to_float(o.get("entry_cost"))
        if entry_cost_total is None:
            entry_cost_total = entry_price * entry_qty_total if entry_price > 0 and entry_qty_total > 0 else None
        exit_cost = _to_float(exit_data.get("exit_cost"))
        entry_fee_total = _to_float(o.get("entry_fee")) or 0.0
        exit_fee = _to_float(exit_data.get("exit_fee")) or 0.0
        matched_fill = meta.get("matched_fill") if isinstance(meta, dict) else None
        mf_qty = _to_float((matched_fill or {}).get("qty")) or 0.0 if isinstance(matched_fill, dict) else 0.0
        mf_cost = _to_float((matched_fill or {}).get("cost")) or 0.0 if isinstance(matched_fill, dict) else 0.0
        mf_fee = _to_float((matched_fill or {}).get("fee")) or 0.0 if isinstance(matched_fill, dict) else 0.0
        if bool(meta.get("reconciled_close")) and exit_qty > 0.0:
            if mf_qty > 0.0 and exit_qty < (mf_qty * 0.999):
                scale = max(0.0, min(1.0, float(exit_qty) / float(mf_qty)))
                if mf_cost > 0.0:
                    exit_cost = mf_cost * scale
                if mf_fee > 0.0:
                    exit_fee = mf_fee * scale
            expected_exit_cost = exit_price * exit_qty if exit_price > 0.0 and exit_qty > 0.0 else None
            if expected_exit_cost is not None:
                if exit_cost is None or exit_cost > (expected_exit_cost * 1.5):
                    exit_cost = expected_exit_cost
        if exit_cost is None:
            exit_cost = exit_price * exit_qty if exit_price > 0 and exit_qty > 0 else None
        close_ratio = 1.0
        if entry_qty_total > 0 and exit_qty > 0:
            close_ratio = min(1.0, float(exit_qty) / float(entry_qty_total))
        entry_qty_closed = float(entry_qty_total) * float(close_ratio) if entry_qty_total > 0 else float(exit_qty)
        entry_cost_closed = (float(entry_cost_total) * float(close_ratio)) if entry_cost_total is not None else None
        entry_fee_closed = float(entry_fee_total) * float(close_ratio)
        gross = None
        if entry_cost_closed is not None and exit_cost is not None:
            gross = float(exit_cost) - float(entry_cost_closed)
        elif entry_price > 0 and exit_price > 0 and entry_qty_closed > 0:
            gross = (float(exit_price) - float(entry_price)) * float(entry_qty_closed)
        net = (float(gross) if gross is not None else 0.0) - float(entry_fee_closed) - float(exit_fee)
        hold_sec = max(0.0, float(closed_ts) - float(o.get("opened_ts") or 0.0)) if o.get("opened_ts") else None
        meta["partial_close"] = bool(close_ratio < 0.999)
        meta["close_ratio"] = float(close_ratio)
        row = {
            "symbol": sym,
            "opened_ts": _to_float(o.get("opened_ts")),
            "closed_ts": float(closed_ts),
            "hold_sec": _to_float(hold_sec),
            "strategy": resolved_strategy or o.get("strategy") or "adopted",
            "source": o.get("source"),
            "signal_name": o.get("signal_name"),
            "signal_id": o.get("signal_id"),
            "req_id": o.get("req_id"),
            "entry_txid": o.get("entry_txid"),
            "exit_txid": exit_data.get("exit_txid"),
            "entry_execution": o.get("entry_execution"),
            "exit_execution": exit_data.get("exit_execution"),
            "entry_price": _to_float(entry_price),
            "exit_price": _to_float(exit_price),
            "entry_qty": _to_float(entry_qty_closed),
            "exit_qty": _to_float(exit_qty),
            "entry_cost": _to_float(entry_cost_closed),
            "exit_cost": _to_float(exit_cost),
            "entry_fee": _to_float(entry_fee_closed),
            "exit_fee": _to_float(exit_fee),
            "fees_total": float(entry_fee_closed) + float(exit_fee),
            "gross_pnl_usd": _to_float(gross),
            "net_pnl_usd": _to_float(net),
            "exit_reason": exit_data.get("exit_reason"),
            "meta_json": json.dumps(meta, separators=(",", ":"), sort_keys=True),
            "created_utc": now,
        }
        conn.execute(
            """
            INSERT INTO closed_trades (
              symbol, opened_ts, closed_ts, hold_sec, strategy, source, signal_name, signal_id, req_id,
              entry_txid, exit_txid, entry_execution, exit_execution, entry_price, exit_price,
              entry_qty, exit_qty, entry_cost, exit_cost, entry_fee, exit_fee, fees_total,
              gross_pnl_usd, net_pnl_usd, exit_reason, meta_json, created_utc
            ) VALUES (
              :symbol, :opened_ts, :closed_ts, :hold_sec, :strategy, :source, :signal_name, :signal_id, :req_id,
              :entry_txid, :exit_txid, :entry_execution, :exit_execution, :entry_price, :exit_price,
              :entry_qty, :exit_qty, :entry_cost, :exit_cost, :entry_fee, :exit_fee, :fees_total,
              :gross_pnl_usd, :net_pnl_usd, :exit_reason, :meta_json, :created_utc
            )
            """,
            row,
        )
        remaining_qty = max(0.0, float(entry_qty_total) - float(exit_qty))
        if remaining_qty <= max(1e-12, float(entry_qty_total) * 0.001):
            conn.execute("DELETE FROM open_trades WHERE symbol=?", (sym,))
        else:
            remaining_ratio = remaining_qty / float(entry_qty_total) if entry_qty_total > 0 else 0.0
            remaining_cost = (float(entry_cost_total) * remaining_ratio) if entry_cost_total is not None else None
            remaining_fee = float(entry_fee_total) * remaining_ratio
            remaining_notional = _to_float(o.get("requested_notional_usd"))
            if remaining_notional is not None:
                remaining_notional = float(remaining_notional) * remaining_ratio
            rem_meta = {}
            try:
                rem_meta = json.loads(o.get("meta_json") or "{}")
            except Exception:
                rem_meta = {}
            rem_meta["remaining_after_partial_exit"] = {
                "qty": remaining_qty,
                "close_ratio": float(close_ratio),
                "closed_ts": float(closed_ts),
            }
            conn.execute(
                """
                UPDATE open_trades SET
                  entry_qty=?,
                  entry_cost=?,
                  entry_fee=?,
                  requested_notional_usd=?,
                  meta_json=?,
                  updated_utc=?
                WHERE symbol=?
                """,
                (remaining_qty, _to_float(remaining_cost), _to_float(remaining_fee), _to_float(remaining_notional), json.dumps(rem_meta, separators=(",", ":"), sort_keys=True), now, sym),
            )
        conn.commit()
    row["ok"] = True
    row["db_path"] = _db_path()
    row["remaining_qty"] = remaining_qty
    row["partial_close"] = bool(remaining_qty > max(1e-12, float(entry_qty_total) * 0.001))
    return row


def list_closed_trades(*, since: Optional[float] = None, limit: int = 200) -> List[Dict[str, Any]]:
    init_db()
    q = "SELECT * FROM closed_trades"
    args: List[Any] = []
    if since is not None:
        q += " WHERE closed_ts >= ?"
        args.append(float(since))
    q += " ORDER BY closed_ts DESC LIMIT ?"
    args.append(max(1, int(limit)))
    with _connect() as conn:
        rows = conn.execute(q, args).fetchall()
    return [dict(r) for r in rows]


def today_realized_pnl_utc(now_ts: Optional[float] = None) -> float:
    now = float(now_ts or time.time())
    g = time.gmtime(now)
    start = time.mktime((g.tm_year, g.tm_mon, g.tm_mday, 0, 0, 0, 0, 0, 0))
    # use calendar.timegm without importing calendar via integer tuple trick
    import calendar
    start = float(calendar.timegm((g.tm_year, g.tm_mon, g.tm_mday, 0, 0, 0)))
    init_db()
    with _connect() as conn:
        row = conn.execute(
            "SELECT COALESCE(SUM(net_pnl_usd), 0.0) AS pnl FROM closed_trades WHERE closed_ts >= ?",
            (start,),
        ).fetchone()
    return float((row[0] if row else 0.0) or 0.0)


def summary(days: float = 7.0) -> Dict[str, Any]:
    init_db()
    since = time.time() - max(0.0, float(days)) * 86400.0
    rows = list_closed_trades(since=since, limit=5000)
    net = 0.0
    gross = 0.0
    fees = 0.0
    wins = 0
    losses = 0
    flat = 0
    avg_win = 0.0
    avg_loss = 0.0
    by_strategy: Dict[str, Dict[str, float]] = {}
    for r in rows:
        pnl = float(r.get("net_pnl_usd") or 0.0)
        gpnl = float(r.get("gross_pnl_usd") or 0.0)
        fee = float(r.get("fees_total") or 0.0)
        strat = str(r.get("strategy") or "unknown")
        net += pnl
        gross += gpnl
        fees += fee
        bucket = by_strategy.setdefault(strat, {"trades": 0.0, "net_pnl_usd": 0.0, "wins": 0.0, "losses": 0.0})
        bucket["trades"] += 1
        bucket["net_pnl_usd"] += pnl
        if pnl > 0:
            wins += 1
            avg_win += pnl
            bucket["wins"] += 1
        elif pnl < 0:
            losses += 1
            avg_loss += pnl
            bucket["losses"] += 1
        else:
            flat += 1
    trades = len(rows)
    return {
        "ok": True,
        "db_path": _db_path(),
        "days": float(days),
        "closed_trades": trades,
        "open_trades": len(list_open_trades(limit=5000)),
        "net_pnl_usd": net,
        "gross_pnl_usd": gross,
        "fees_total_usd": fees,
        "wins": wins,
        "losses": losses,
        "flat": flat,
        "win_rate": (wins / (wins + losses)) if (wins + losses) > 0 else None,
        "avg_win_usd": (avg_win / wins) if wins > 0 else None,
        "avg_loss_usd": (avg_loss / losses) if losses > 0 else None,
        "by_strategy": by_strategy,
    }


def _to_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        return float(x)
    except Exception:
        return None
