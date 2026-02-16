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
