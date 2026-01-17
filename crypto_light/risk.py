from __future__ import annotations


def compute_brackets(entry_price: float, stop_pct: float, take_pct: float) -> tuple[float, float]:
    if entry_price <= 0:
        raise ValueError("entry_price must be > 0")
    if stop_pct <= 0 or take_pct <= 0:
        raise ValueError("stop_pct and take_pct must be > 0")
    stop_price = entry_price * (1.0 - stop_pct)
    take_price = entry_price * (1.0 + take_pct)
    return stop_price, take_price
