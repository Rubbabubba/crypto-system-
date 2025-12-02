# position_manager.py
# Lightweight position engine built from the same trades data used for PnL.
# Now also provides a unified Position Manager API used by the scheduler.

from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, Tuple, List, Any, Optional, Callable
import sqlite3


@dataclass
class Position:
    symbol: str
    strategy: str
    qty: float
    avg_price: Optional[float] = None


@dataclass
class OrderIntent:
    """
    High-level execution intent for a single market order.
    """
    symbol: str
    strategy: str
    side: str       # "buy" or "sell"
    notional: float
    intent: str     # "open_long", "close_short", "flip_long_to_short", etc.


def load_net_positions(
    con,
    table: str = "trades",
    use_strategy_col: bool = False,
) -> Dict[Tuple[str, str], Position]:
    """
    Build net positions from the trades table.

    - Long-only spot model.
    - avg_price is a simple volume-weighted average of all *net* buys
      remaining after sells.

    We ignore 'cost' and compute everything from price * volume.
    """
    cur = con.cursor()

    # We need at least: symbol, side, price, volume, ts
    cols = ["symbol", "side", "price", "volume", "ts"]
    if use_strategy_col:
        cols.append("strategy")

    col_sql = ", ".join(cols)
    cur.execute(
        f"""
        SELECT {col_sql}
        FROM {table}
        ORDER BY ts ASC
        """
    )

    state: Dict[Tuple[str, str], Dict[str, float]] = {}

    for row in cur.fetchall():
        # unpack based on whether we have strategy or not
        if use_strategy_col:
            symbol, side, price, volume, ts, strategy = row
            strategy = strategy or "misc"
        else:
            symbol, side, price, volume, ts = row
            strategy = "misc"

        key = (symbol, strategy)
        price = float(price or 0.0)
        volume = float(volume or 0.0)

        # skip zero volume
        if abs(volume) < 1e-12:
            continue

        s = state.get(key) or {"qty": 0.0, "avg_price": 0.0}
        qty = s["qty"]
        avg_price = s["avg_price"]

        if side == "buy":
            # add to long
            new_qty = qty + volume
            if new_qty <= 0:
                # fully flipped or flat => reset avg_price
                qty = new_qty
                avg_price = 0.0
            else:
                new_cost = qty * avg_price + volume * price
                qty = new_qty
                avg_price = new_cost / qty
        elif side == "sell":
            # reduce long
            new_qty = qty - volume
            if new_qty <= 1e-10:
                # position closed (or tiny dust) -> treat as flat
                qty = 0.0
                avg_price = 0.0
            else:
                # closing part of a long position:
                # keep the same avg_price for remaining qty
                qty = new_qty
        else:
            # unknown side; ignore
            continue

        s["qty"] = qty
        s["avg_price"] = avg_price
        state[key] = s

    # Build Position objects for non-zero qty (with dust cut off)
    positions: Dict[Tuple[str, str], Position] = {}
    for (symbol, strategy), s in state.items():
        qty = s["qty"]
        avg_price = s["avg_price"]
        if abs(qty) <= 1e-8:
            continue  # ignore dust
        positions[(symbol, strategy)] = Position(
            symbol=symbol,
            strategy=strategy,
            qty=qty,
            avg_price=avg_price if avg_price > 0 else None,
        )

    return positions

# ---------------------------------------------------------------------------
# Unified Position Manager
# ---------------------------------------------------------------------------

def position_side(qty: float, eps: float = 1e-10) -> str:
    """
    Map a signed quantity to a discrete side:
      > 0 -> "long"
      < 0 -> "short"
      ~=0 -> "flat"
    """
    if qty > eps:
        return "long"
    if qty < -eps:
        return "short"
    return "flat"


def _notional_for_qty(
    symbol: str,
    qty: float,
    price_lookup: Optional[Callable[[str], float]],
) -> float:
    """
    Convert a base quantity into USD notional using a price lookup function.
    Returns 0 if price not available.
    """
    if price_lookup is None:
        return 0.0
    try:
        px = float(price_lookup(symbol) or 0.0)
    except Exception:
        px = 0.0
    return abs(qty) * px if px > 0 else 0.0


# --- Atomic intents --------------------------------------------------------

def open_long(symbol: str, strategy: str, target_notional: float) -> OrderIntent:
    """
    Open a new long position from flat.
    """
    return OrderIntent(
        symbol=symbol,
        strategy=strategy,
        side="buy",
        notional=float(target_notional),
        intent="open_long",
    )


def open_short(symbol: str, strategy: str, target_notional: float) -> OrderIntent:
    """
    Open a new short position from flat.
    """
    return OrderIntent(
        symbol=symbol,
        strategy=strategy,
        side="sell",
        notional=float(target_notional),
        intent="open_short",
    )


def close_long(
    symbol: str,
    strategy: str,
    current_qty: float,
    price_lookup: Optional[Callable[[str], float]] = None,
) -> Optional[OrderIntent]:
    """
    Close an existing long position (qty > 0) back to flat.
    """
    if current_qty <= 0:
        return None
    notional = _notional_for_qty(symbol, current_qty, price_lookup)
    if notional <= 0:
        return None
    return OrderIntent(
        symbol=symbol,
        strategy=strategy,
        side="sell",
        notional=notional,
        intent="close_long",
    )


def close_short(
    symbol: str,
    strategy: str,
    current_qty: float,
    price_lookup: Optional[Callable[[str], float]] = None,
) -> Optional[OrderIntent]:
    """
    Close an existing short position (qty < 0) back to flat.
    """
    if current_qty >= 0:
        return None
    notional = _notional_for_qty(symbol, current_qty, price_lookup)
    if notional <= 0:
        return None
    return OrderIntent(
        symbol=symbol,
        strategy=strategy,
        side="buy",
        notional=notional,
        intent="close_short",
    )


def flip_long_to_short(
    symbol: str,
    strategy: str,
    current_qty: float,
    target_notional: float,
    price_lookup: Optional[Callable[[str], float]] = None,
) -> List[OrderIntent]:
    """
    Flip from long to short:
      1) close the existing long
      2) open a new short of target_notional
    """
    out: List[OrderIntent] = []
    c = close_long(symbol, strategy, current_qty, price_lookup)
    if c:
        out.append(c)
    if target_notional > 0:
        out.append(open_short(symbol, strategy, target_notional))
    return out


def flip_short_to_long(
    symbol: str,
    strategy: str,
    current_qty: float,
    target_notional: float,
    price_lookup: Optional[Callable[[str], float]] = None,
) -> List[OrderIntent]:
    """
    Flip from short to long:
      1) close the existing short
      2) open a new long of target_notional
    """
    out: List[OrderIntent] = []
    c = close_short(symbol, strategy, current_qty, price_lookup)
    if c:
        out.append(c)
    if target_notional > 0:
        out.append(open_long(symbol, strategy, target_notional))
    return out


# --- High-level planner ----------------------------------------------------

def plan_position_adjustment(
    symbol: str,
    strategy: str,
    current_qty: float,
    desired: str,
    target_notional: float,
    price_lookup: Optional[Callable[[str], float]] = None,
) -> List[OrderIntent]:
    """
    Given the current net qty and a desired action:
      - desired = "buy"  -> want to be long
      - desired = "sell" -> want to be short
      - desired = "flat" -> want to be flat

    Returns a sequence of OrderIntent objects such that:
      - We never pyramid within a strategy (no add-ons to an existing position).
      - Flips are explicit (close then open).
      - Close-only actions do not overtrade.

    This is what the scheduler should call.
    """
    desired = (desired or "").lower().strip()
    if desired not in ("buy", "sell", "flat"):
        return []

    side = position_side(current_qty)

    # Case 0: Already flat
    if side == "flat":
        if desired == "buy" and target_notional > 0:
            return [open_long(symbol, strategy, target_notional)]
        if desired == "sell" and target_notional > 0:
            return [open_short(symbol, strategy, target_notional)]
        # desired flat and already flat -> nothing to do
        return []

    # Case 1: Currently long
    if side == "long":
        if desired == "buy":
            # Already long and we forbid pyramiding: no action.
            return []
        if desired == "sell":
            # Flip long -> short
            return flip_long_to_short(symbol, strategy, current_qty, target_notional, price_lookup)
        if desired == "flat":
            # Close the long only
            c = close_long(symbol, strategy, current_qty, price_lookup)
            return [c] if c else []
        return []

    # Case 2: Currently short
    if side == "short":
        if desired == "sell":
            # Already short and we forbid pyramiding: no action.
            return []
        if desired == "buy":
            # Flip short -> long
            return flip_short_to_long(symbol, strategy, current_qty, target_notional, price_lookup)
        if desired == "flat":
            # Close the short only
            c = close_short(symbol, strategy, current_qty, price_lookup)
            return [c] if c else []
        return []

    return []
