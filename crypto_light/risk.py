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


def compute_stop_distance_pct(entry_price: float, stop_price: float) -> float:
    px = float(entry_price or 0.0)
    stop = float(stop_price or 0.0)
    if px <= 0.0 or stop <= 0.0 or stop >= px:
        return 0.0
    return max(0.0, (px - stop) / px)


def compute_take_distance_pct(entry_price: float, take_price: float) -> float:
    px = float(entry_price or 0.0)
    take = float(take_price or 0.0)
    if px <= 0.0 or take <= px:
        return 0.0
    return max(0.0, (take - px) / px)


def compute_rr_ratio(entry_price: float, stop_price: float, take_price: float) -> float:
    risk_pct = compute_stop_distance_pct(entry_price, stop_price)
    reward_pct = compute_take_distance_pct(entry_price, take_price)
    if risk_pct <= 0.0:
        return 0.0
    return max(0.0, reward_pct / risk_pct)


def compute_effective_stop_pct(
    *,
    entry_price: float,
    stop_price: float,
    entry_fee_bps: float = 0.0,
    exit_fee_bps: float = 0.0,
    slippage_bps: float = 0.0,
) -> float:
    stop_pct = compute_stop_distance_pct(entry_price, stop_price)
    friction_pct = (float(entry_fee_bps or 0.0) + float(exit_fee_bps or 0.0) + float(slippage_bps or 0.0)) / 10000.0
    return max(0.0, stop_pct + friction_pct)
