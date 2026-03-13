from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Dict


@dataclass(frozen=True)
class SizingResult:
    ok: bool
    notional_usd: float = 0.0
    equity_usd: float = 0.0
    risk_usd: float = 0.0
    target_notional_usd: float = 0.0
    capped_by: str = ""
    reason: str = ""

    def to_dict(self) -> Dict[str, float | str | bool]:
        return {
            "ok": self.ok,
            "notional_usd": float(self.notional_usd),
            "equity_usd": float(self.equity_usd),
            "risk_usd": float(self.risk_usd),
            "target_notional_usd": float(self.target_notional_usd),
            "capped_by": self.capped_by,
            "reason": self.reason,
        }



def compute_equity_fraction_notional(
    *,
    equity_usd: float,
    fraction: float,
    min_order_notional_usd: float,
    available_cash_usd: float,
    max_total_exposure_usd: float = 0.0,
    current_total_exposure_usd: float = 0.0,
    max_symbol_exposure_usd: float = 0.0,
    current_symbol_exposure_usd: float = 0.0,
    max_notional_usd: float = 0.0,
) -> SizingResult:
    """Size position as a fraction of total account equity (spot-friendly).

    equity_usd should represent total account value (cash + open positions).
    """
    try:
        equity = float(equity_usd or 0.0)
        frac = float(fraction or 0.0)
    except Exception:
        return SizingResult(ok=False, reason="bad_inputs")

    if equity <= 0.0:
        return SizingResult(ok=False, equity_usd=equity, reason="no_equity")
    if frac <= 0.0:
        return SizingResult(ok=False, equity_usd=equity, reason="fraction_disabled")

    target = equity * frac
    notional = target
    capped_by = ""

    # Optional hard cap.
    if max_notional_usd and max_notional_usd > 0 and notional > max_notional_usd:
        notional = float(max_notional_usd)
        capped_by = "max_notional"

    # Cap by remaining total exposure.
    if max_total_exposure_usd and max_total_exposure_usd > 0:
        remaining = float(max_total_exposure_usd) - float(current_total_exposure_usd or 0.0)
        if remaining <= 0:
            return SizingResult(ok=False, equity_usd=equity, target_notional_usd=target, reason="total_exposure_cap")
        if notional > remaining:
            notional = remaining
            capped_by = capped_by or "total_exposure_cap"

    # Cap by remaining symbol exposure.
    if max_symbol_exposure_usd and max_symbol_exposure_usd > 0:
        remaining_sym = float(max_symbol_exposure_usd) - float(current_symbol_exposure_usd or 0.0)
        if remaining_sym <= 0:
            return SizingResult(ok=False, equity_usd=equity, target_notional_usd=target, reason="symbol_exposure_cap")
        if notional > remaining_sym:
            notional = remaining_sym
            capped_by = capped_by or "symbol_exposure_cap"

    # Cap by available cash (spot).
    cash = float(available_cash_usd or 0.0)
    if cash <= 0.0:
        return SizingResult(ok=False, equity_usd=equity, target_notional_usd=target, reason="no_cash")
    if notional > cash:
        notional = cash
        capped_by = capped_by or "cash_cap"

    # Minimum notional guard.
    min_n = float(min_order_notional_usd or 0.0)
    if min_n > 0.0 and notional < min_n:
        # If the computed notional falls below the exchange minimum, try to *bump up* to the minimum
        # when feasible under caps. This is important for small accounts where equity*fraction
        # is often slightly below the minimum order size.
        feasible = True

        # Respect hard max_notional cap.
        if max_notional_usd and max_notional_usd > 0 and min_n > float(max_notional_usd):
            feasible = False

        # Respect exposure caps.
        if feasible and max_total_exposure_usd and max_total_exposure_usd > 0:
            remaining = float(max_total_exposure_usd) - float(current_total_exposure_usd or 0.0)
            if min_n > remaining:
                feasible = False

        if feasible and max_symbol_exposure_usd and max_symbol_exposure_usd > 0:
            remaining_sym = float(max_symbol_exposure_usd) - float(current_symbol_exposure_usd or 0.0)
            if min_n > remaining_sym:
                feasible = False

        # Respect available cash.
        if feasible and cash < min_n:
            feasible = False

        if not feasible:
            return SizingResult(
                ok=False,
                equity_usd=equity,
                target_notional_usd=target,
                notional_usd=notional,
                reason="below_min_notional",
            )

        notional = min_n
        capped_by = capped_by or "min_notional_bump"

    return SizingResult(
        ok=True,
        notional_usd=float(notional),
        equity_usd=float(equity),
        risk_usd=0.0,
        target_notional_usd=float(target),
        capped_by=capped_by,
        reason="",
    )

def compute_risk_pct_equity_notional(
    *,
    equity_usd: float,
    risk_per_trade: float,
    stop_pct: float,
    min_order_notional_usd: float,
    available_cash_usd: float,
    max_total_exposure_usd: float,
    current_total_exposure_usd: float,
    max_symbol_exposure_usd: float,
    current_symbol_exposure_usd: float,
    max_notional_usd: float,
) -> SizingResult:
    # Basic validation
    if equity_usd <= 0:
        return SizingResult(ok=False, reason="no_equity_usd", equity_usd=equity_usd)
    if stop_pct <= 0:
        return SizingResult(ok=False, reason="invalid_stop_pct", equity_usd=equity_usd)
    if risk_per_trade <= 0:
        return SizingResult(ok=False, reason="invalid_risk_per_trade", equity_usd=equity_usd)

    if available_cash_usd <= 0:
        return SizingResult(ok=False, reason="no_cash", equity_usd=equity_usd)

    risk_usd = float(equity_usd) * float(risk_per_trade)
    target = risk_usd / float(stop_pct)

    # Build caps
    caps = []

    capped_by = ""
    # Cash cap
    if available_cash_usd > 0:
        caps.append(("cash", float(available_cash_usd)))
    # Total exposure cap
    if max_total_exposure_usd > 0:
        remaining = float(max_total_exposure_usd) - float(current_total_exposure_usd)
        caps.append(("max_total_exposure_usd", max(0.0, remaining)))
    # Symbol exposure cap
    if max_symbol_exposure_usd > 0:
        remaining = float(max_symbol_exposure_usd) - float(current_symbol_exposure_usd)
        caps.append(("max_symbol_exposure_usd", max(0.0, remaining)))
    # Optional absolute cap
    if max_notional_usd and max_notional_usd > 0:
        caps.append(("max_notional_usd", float(max_notional_usd)))

    final = float(target)
    for name, cap in caps:
        if cap < final:
            final = cap
            capped_by = name

    if final <= 0:
        return SizingResult(
            ok=False,
            reason="notional_capped_to_zero",
            equity_usd=equity_usd,
            risk_usd=risk_usd,
            target_notional_usd=target,
            capped_by=capped_by,
        )

    if final < float(min_order_notional_usd):
        return SizingResult(
            ok=False,
            reason="notional_below_minimum",
            equity_usd=equity_usd,
            risk_usd=risk_usd,
            target_notional_usd=target,
            notional_usd=final,
            capped_by=capped_by,
        )

    return SizingResult(
        ok=True,
        notional_usd=final,
        equity_usd=equity_usd,
        risk_usd=risk_usd,
        target_notional_usd=target,
        capped_by=capped_by,
    )

def compute_risk_based_notional_actual(
    *,
    equity_usd: float,
    risk_per_trade: float,
    effective_stop_pct: float,
    min_order_notional_usd: float,
    available_cash_usd: float,
    max_total_exposure_usd: float,
    current_total_exposure_usd: float,
    max_symbol_exposure_usd: float,
    current_symbol_exposure_usd: float,
    max_notional_usd: float,
) -> SizingResult:
    if equity_usd <= 0:
        return SizingResult(ok=False, reason="no_equity_usd", equity_usd=float(equity_usd or 0.0))
    if risk_per_trade <= 0:
        return SizingResult(ok=False, reason="invalid_risk_per_trade", equity_usd=float(equity_usd or 0.0))
    if effective_stop_pct <= 0:
        return SizingResult(ok=False, reason="invalid_effective_stop_pct", equity_usd=float(equity_usd or 0.0))
    if available_cash_usd <= 0:
        return SizingResult(ok=False, reason="no_cash", equity_usd=float(equity_usd or 0.0))

    risk_usd = float(equity_usd) * float(risk_per_trade)
    target = risk_usd / float(effective_stop_pct)
    final = float(target)
    capped_by = ""

    caps = [("cash", float(available_cash_usd))]
    if max_total_exposure_usd > 0:
        remaining = float(max_total_exposure_usd) - float(current_total_exposure_usd or 0.0)
        caps.append(("max_total_exposure_usd", max(0.0, remaining)))
    if max_symbol_exposure_usd > 0:
        remaining_sym = float(max_symbol_exposure_usd) - float(current_symbol_exposure_usd or 0.0)
        caps.append(("max_symbol_exposure_usd", max(0.0, remaining_sym)))
    if max_notional_usd and max_notional_usd > 0:
        caps.append(("max_notional_usd", float(max_notional_usd)))

    for name, cap in caps:
        if cap < final:
            final = cap
            capped_by = name

    if final <= 0:
        return SizingResult(
            ok=False,
            reason="notional_capped_to_zero",
            equity_usd=float(equity_usd),
            risk_usd=float(risk_usd),
            target_notional_usd=float(target),
            capped_by=capped_by,
        )

    if final < float(min_order_notional_usd or 0.0):
        return SizingResult(
            ok=False,
            reason="notional_below_minimum",
            equity_usd=float(equity_usd),
            risk_usd=float(risk_usd),
            target_notional_usd=float(target),
            notional_usd=float(final),
            capped_by=capped_by,
        )

    return SizingResult(
        ok=True,
        notional_usd=float(final),
        equity_usd=float(equity_usd),
        risk_usd=float(risk_usd),
        target_notional_usd=float(target),
        capped_by=capped_by,
        reason="",
    )
