from __future__ import annotations

import time
from typing import Any, Dict, Optional


def extract_broker_txid(res: Dict[str, Any] | None) -> Optional[str]:
    res = dict(res or {})
    txids = res.get("txids") or res.get("txid") or res.get("order_txid")
    if isinstance(txids, list):
        return str(txids[0]) if txids else None
    if txids:
        return str(txids)
    maker = res.get("maker_first") or {}
    last_err = maker.get("last_error") or {}
    txid = last_err.get("txid")
    return str(txid) if txid else None


def classify_order_result(res: Dict[str, Any] | None) -> Dict[str, Any]:
    """Best-effort lifecycle classification for broker responses.

    Returns one of: filled, partial, acknowledged, cancelled, rejected, failed_reconcile.
    """
    res = dict(res or {})
    txid = extract_broker_txid(res)
    reconciled = dict(res.get("reconciled") or {})
    maker_first = dict(res.get("maker_first") or {})
    last_err = dict(maker_first.get("last_error") or {})
    stop_limit_first = dict(res.get("stop_limit_first") or {})

    try:
        filled_qty = float(reconciled.get("vol_exec") or reconciled.get("volume") or 0.0)
    except Exception:
        filled_qty = 0.0
    if filled_qty <= 0:
        try:
            filled_qty = float(last_err.get("vol_exec") or 0.0)
        except Exception:
            filled_qty = 0.0
    if filled_qty <= 0:
        try:
            filled_qty = float((stop_limit_first.get("reconciled") or {}).get("vol_exec") or 0.0)
        except Exception:
            filled_qty = 0.0

    try:
        avg_price = float(reconciled.get("avg_price") or reconciled.get("price") or 0.0)
    except Exception:
        avg_price = 0.0
    if avg_price <= 0:
        try:
            avg_price = float(last_err.get("limit_price") or 0.0)
        except Exception:
            avg_price = 0.0

    try:
        fees_usd = float(reconciled.get("fee") or 0.0)
    except Exception:
        fees_usd = 0.0

    ok = bool(res.get("ok"))
    filled = bool(res.get("filled")) or bool(reconciled.get("filled"))
    error = str(res.get("error") or "")
    execution = str(res.get("execution") or "")

    if ok and filled:
        state = "filled"
    elif filled_qty > 0 and not ok:
        state = "partial"
    elif txid and ok:
        state = "acknowledged"
    elif txid and ("not_filled" in error or "timeout" in error):
        state = "cancelled"
    elif txid and error:
        state = "failed_reconcile"
    elif error:
        state = "rejected"
    else:
        state = "submitted"

    return {
        "state": state,
        "broker_txid": txid,
        "filled_qty": float(filled_qty),
        "avg_fill_price": float(avg_price),
        "fees_usd": float(fees_usd),
        "error": error,
        "execution": execution,
        "updated_ts": time.time(),
    }


def state_counts(rows: list[dict[str, Any]] | None) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for row in rows or []:
        state = str((row or {}).get("state") or "unknown")
        counts[state] = counts.get(state, 0) + 1
    return counts
