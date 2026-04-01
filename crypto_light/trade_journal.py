from __future__ import annotations

import json
import os
import sqlite3
import time
from typing import Any, Dict, List, Optional


def _db_path() -> str:
    return os.getenv("TRADE_JOURNAL_DB_PATH", ".data/trade_journal.sqlite3")


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
        existing_closed = _find_closed_by_symbol_exit_txid(conn, sym, exit_txid) if exit_txid else None
        if existing_closed is not None and (bool(meta.get("reconciled_close")) or str(exit_data.get("exit_reason") or "").startswith("reconciled_fill")):
            try:
                conn.execute("DELETE FROM open_trades WHERE symbol=?", (sym,))
                conn.commit()
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
            "strategy": o.get("strategy"),
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
