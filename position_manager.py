# position_manager.py
# Lightweight position engine built from the same trades data used for PnL.
# This is intentionally simple: it only tracks net qty per symbol (and optionally per strategy).

from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, Tuple, List, Any
import sqlite3


@dataclass
class Position:
    symbol: str
    strategy: str
    qty: float      # net signed quantity (>0 long, <0 short)
    avg_price: float | None = None  # optional; we mainly need qty


def load_net_positions(
    conn: sqlite3.Connection,
    table: str = "trades",
    use_strategy_col: bool = False,
) -> Dict[Tuple[str, str], Position]:
    """
    Build net positions by (symbol, strategy) or just (symbol, 'misc') if strategy col not used.

    Assumes table has columns:
      - symbol (text)
      - side   ('buy' / 'sell')
      - volume (real)
      - price  (real)
      - strategy (optional)
    """
    # Detect columns dynamically
    cols = [r[1] for r in conn.execute(f"PRAGMA table_info({table})")]
    has_strategy = "strategy" in cols and use_strategy_col

    if not all(c in cols for c in ("symbol", "side", "volume", "price")):
        return {}

    if has_strategy:
        sql = f"SELECT symbol, COALESCE(NULLIF(TRIM(strategy),''),'misc'), side, volume, price FROM {table} ORDER BY ts"
    else:
        sql = f"SELECT symbol, 'misc' as strategy, side, volume, price FROM {table} ORDER BY ts"

    positions: Dict[Tuple[str, str], Position] = {}
    cur = conn.execute(sql)
    for symbol, strategy, side, volume, price in cur.fetchall():
        if not symbol or not side:
            continue
        try:
            qty = float(volume or 0.0)
            px = float(price or 0.0)
        except Exception:
            continue

        side = side.lower().strip()
        signed = qty if side == "buy" else -qty

        key = (symbol, strategy)
        pos = positions.get(key)
        if pos is None:
            positions[key] = Position(symbol=symbol, strategy=strategy, qty=signed, avg_price=px if signed != 0 else None)
            continue

        # Update existing position
        new_qty = pos.qty + signed

        # Update avg price only if position stays on same side
        if pos.qty == 0 or (pos.qty > 0 and new_qty > 0) or (pos.qty < 0 and new_qty < 0):
            # size-weighted average price
            try:
                total_notional_old = abs(pos.qty) * (pos.avg_price or 0.0)
                total_notional_new = abs(signed) * px
                if abs(new_qty) > 0:
                    avg_px = (total_notional_old + total_notional_new) / abs(new_qty)
                else:
                    avg_px = None
            except Exception:
                avg_px = pos.avg_price
        else:
            # We crossed through flat (partial or full close); for simplicity: keep old avg_price
            avg_px = pos.avg_price

        pos.qty = new_qty
        pos.avg_price = avg_px

    # Filter out positions that are effectively flat
    out: Dict[Tuple[str, str], Position] = {}
    for key, pos in positions.items():
        if abs(pos.qty) > 1e-10:
            out[key] = pos
    return out
