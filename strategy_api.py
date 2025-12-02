"""
strategy_api.py — shared strategy interfaces (v0.1.0)

This module defines the common types and contracts used by all strategies.

Each concrete strategy (c1–c6) should implement:
- entry_signal()
- exit_signal()
- profit_take_rule()
- stop_loss_rule()
- should_scale()
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Literal, Protocol

from book import ScanResult  # existing dataclass from book.py

Side = Literal["buy", "sell"]
IntentKind = Literal["entry", "exit", "take_profit", "stop_loss", "scale"]


@dataclass
class PositionSnapshot:
    """
    Lightweight view of a strategy's position in a symbol.

    This is derived from your trades / journal table via position_manager.
    """
    symbol: str
    strategy: str
    qty: float = 0.0                 # >0 long, <0 short
    avg_price: Optional[float] = None
    unrealized_pct: Optional[float] = None  # +0.025 for +2.5%


@dataclass
class RiskContext:
    """
    Read-only view of relevant risk configuration.

    For now:
    - fee_rate_pct: trading fee as decimal (e.g. 0.0026 = 0.26%)
    - edge_multiple_vs_fee: required edge vs fees
    """
    fee_rate_pct: float = 0.0026
    edge_multiple_vs_fee: float = 2.0
    raw_config: Dict[str, Any] = field(default_factory=dict)


@dataclass
class OrderIntent:
    """
    A single, high-level order idea from a strategy.

    notional:
      * For ENTRY / SCALE intents: target USD size (e.g. 40).
      * For EXIT / TAKE_PROFIT / STOP_LOSS:
          - None => "close entire position"
          - > 0  => "close this much notional"
    """
    strategy: str
    symbol: str
    side: Side
    kind: IntentKind
    notional: Optional[float] = None
    reason: str = ""
    meta: Dict[str, Any] = field(default_factory=dict)


class BaseStrategy(Protocol):
    """
    Protocol that concrete strategies (c1–c6) should follow.
    """

    STRAT_ID: str

    def entry_signal(
        self,
        scan: ScanResult,
        position: PositionSnapshot,
        risk: Optional[RiskContext] = None,
    ) -> Optional[OrderIntent]:
        ...

    def exit_signal(
        self,
        scan: ScanResult,
        position: PositionSnapshot,
        risk: Optional[RiskContext] = None,
    ) -> Optional[OrderIntent]:
        ...

    def profit_take_rule(
        self,
        position: PositionSnapshot,
        risk: Optional[RiskContext] = None,
    ) -> Optional[OrderIntent]:
        ...

    def stop_loss_rule(
        self,
        position: PositionSnapshot,
        risk: Optional[RiskContext] = None,
    ) -> Optional[OrderIntent]:
        ...

    def should_scale(
        self,
        scan: ScanResult,
        position: PositionSnapshot,
        risk: Optional[RiskContext] = None,
    ) -> bool:
        ...
