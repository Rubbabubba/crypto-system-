from __future__ import annotations


def compute_brackets(entry_price: float, stop_pct: float, take_pct: float) -> tuple[float, float]:
    if entry_price <= 0:
        raise ValueError("entry_price must be > 0")
    if stop_pct <= 0 or take_pct <= 0:
        raise ValueError("stop_pct and take_pct must be > 0")
    stop_price = entry_price * (1.0 - stop_pct)
    take_price = entry_price * (1.0 + take_pct)
    return stop_price, take_price


def compute_atr_brackets(entry_price: float, atr: float, stop_mult: float, take_mult: float) -> tuple[float, float]:
    """Return (stop, take) based on ATR in price units.

    stop = entry - atr*stop_mult
    take = entry + atr*take_mult
    """
    px = float(entry_price)
    a = float(atr)
    s = float(stop_mult)
    t = float(take_mult)
    stop = px - a * s
    take = px + a * t
    # Safety guards
    if stop <= 0:
        stop = max(px * 0.5, 0.00000001)
    if take <= px:
        take = px * 1.01
    return float(stop), float(take)
