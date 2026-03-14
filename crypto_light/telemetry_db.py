from __future__ import annotations

import json
import os
import sqlite3
import time
from typing import Any, Dict, List, Optional

from . import trade_journal


def _db_path() -> str:
    return os.getenv("TELEMETRY_DB_PATH", "/var/data/telemetry.sqlite3")


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(_db_path())
    conn.row_factory = sqlite3.Row
    return conn


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
        conn.execute("CREATE INDEX IF NOT EXISTS idx_trade_telemetry_closed_ts ON trade_telemetry(closed_ts DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_trade_telemetry_symbol_closed_ts ON trade_telemetry(symbol, closed_ts DESC)")
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
    return "|".join([
        str(row.get("symbol") or ""),
        str(row.get("opened_ts") or ""),
        str(row.get("closed_ts") or ""),
        str(row.get("entry_txid") or ""),
        str(row.get("exit_txid") or ""),
    ])


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


def sync_from_trade_journal(limit: int = 500) -> Dict[str, Any]:
    init_db()
    rows = trade_journal.list_closed_trades(limit=max(1, int(limit)))
    expected_slippage_bps = _env_float("EXPECTED_SLIPPAGE_BPS", _env_float("SLIPPAGE_BPS", 8.0))
    max_realized_slippage_bps_alert = _env_float("MAX_REALIZED_SLIPPAGE_BPS_ALERT", 35.0)
    max_entry_fee_bps_alert = _env_float("MAX_ENTRY_FEE_BPS_ALERT", 40.0)
    max_exit_fee_bps_alert = _env_float("MAX_EXIT_FEE_BPS_ALERT", 40.0)
    inserted = 0
    with _connect() as conn:
        for r in rows:
            meta = _safe_json_loads(r.get("meta_json"))
            order_result = _safe_json_loads(meta.get("order_result"))
            entry_ctx = _safe_json_loads(meta.get("entry_context"))
            # support direct top-level fallback too
            entry_ref_price = _to_float(meta.get("entry_ref_price"))
            if entry_ref_price is None:
                entry_ref_price = _to_float(entry_ctx.get("entry_ref_price"))
            exit_ref_price = _to_float(meta.get("exit_ref_price"))
            partial_close = bool(meta.get("partial_close") or False)
            entry_fill_price = _to_float(r.get("entry_price"))
            exit_fill_price = _to_float(r.get("exit_price"))
            entry_fee_bps = _fee_bps(_to_float(r.get("entry_fee")), _to_float(r.get("entry_cost")))
            exit_fee_bps = _fee_bps(_to_float(r.get("exit_fee")), _to_float(r.get("exit_cost")))
            entry_slip = _slippage_bps(entry_fill_price, entry_ref_price)
            exit_slip = _slippage_bps(exit_fill_price, exit_ref_price)
            slips = [x for x in [entry_slip, exit_slip] if x is not None]
            max_slip = max(slips) if slips else None
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
            clean_trade = int(len(alerts) == 0 and bool(r.get("exit_txid")) and bool(r.get("entry_txid")))
            payload = {
                "trade_key": _trade_key(r),
                "symbol": str(r.get("symbol") or ""),
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
                "entry_execution": r.get("entry_execution"),
                "exit_execution": r.get("exit_execution"),
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
                    "source_meta": meta,
                }, separators=(",", ":"), sort_keys=True),
                "created_utc": time.time(),
                "updated_utc": time.time(),
            }
            conn.execute(
                """
                INSERT INTO trade_telemetry (
                  trade_key, symbol, strategy, source, signal_name, signal_id, req_id, opened_ts, closed_ts, hold_sec,
                  entry_txid, exit_txid, entry_execution, exit_execution, entry_ref_price, exit_ref_price,
                  entry_fill_price, exit_fill_price, entry_qty, exit_qty, entry_cost, exit_cost, entry_fee, exit_fee,
                  entry_fee_bps, exit_fee_bps, entry_realized_slippage_bps, exit_realized_slippage_bps, max_realized_slippage_bps,
                  fees_total, gross_pnl_usd, net_pnl_usd, exit_reason, partial_close, clean_trade, alert_flags_json, meta_json, created_utc, updated_utc
                ) VALUES (
                  :trade_key, :symbol, :strategy, :source, :signal_name, :signal_id, :req_id, :opened_ts, :closed_ts, :hold_sec,
                  :entry_txid, :exit_txid, :entry_execution, :exit_execution, :entry_ref_price, :exit_ref_price,
                  :entry_fill_price, :exit_fill_price, :entry_qty, :exit_qty, :entry_cost, :exit_cost, :entry_fee, :exit_fee,
                  :entry_fee_bps, :exit_fee_bps, :entry_realized_slippage_bps, :exit_realized_slippage_bps, :max_realized_slippage_bps,
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
