# position_engine.py
# Position-aware PnL engine based on raw fills in `trades` table.
from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, List, Tuple, Iterable, Optional, Callable


@dataclass
class Fill:
    ts: float
    symbol: str
    side: str       # "buy" or "sell"
    qty: float
    price: float
    fee: float
    strategy: str   # e.g. "c2", "c3", "misc"
    txid: str = ""


@dataclass
class Lot:
    qty: float      # signed: >0 long, <0 short
    price: float    # entry price


@dataclass
class Aggregate:
    realized: float = 0.0
    fees: float = 0.0

    def equity(self, unreal: float) -> float:
        return self.realized + unreal - self.fees


class PositionEngine:
    """
    Stateless engine: feed fills in time order, then query aggregates.

    - Positions are tracked per (symbol, strategy) pair.
    - FIFO lot accounting with signed quantities.
    - Long PnL: (exit_price - entry_price) * qty
    - Short PnL: (entry_price - exit_price) * qty
    """

    def __init__(self) -> None:
        self._lots: Dict[Tuple[str, str], List[Lot]] = {}
        self._agg: Dict[Tuple[str, str], Aggregate] = {}

    def _get_agg(self, key: Tuple[str, str]) -> Aggregate:
        if key not in self._agg:
            self._agg[key] = Aggregate()
        return self._agg[key]

    def _get_lots(self, key: Tuple[str, str]) -> List[Lot]:
        if key not in self._lots:
            self._lots[key] = []
        return self._lots[key]

    def process_fill(self, f: Fill) -> None:
        side = f.side.lower().strip()
        if side not in ("buy", "sell"):
            return

        key = (f.symbol, f.strategy or "misc")
        lots = self._get_lots(key)
        agg = self._get_agg(key)

        qty_signed = f.qty if side == "buy" else -f.qty
        if qty_signed == 0:
            return

        agg.fees += f.fee or 0.0

        # If no open position or same direction, add to lots
        if not lots or (lots[0].qty >= 0 and qty_signed >= 0) or (lots[0].qty <= 0 and qty_signed <= 0):
            lots.append(Lot(qty=qty_signed, price=f.price))
            return

        # Otherwise we are closing (part of) an existing position
        remaining = qty_signed

        while remaining != 0 and lots:
            lot = lots[0]
            close_qty = min(abs(lot.qty), abs(remaining))
            if close_qty == 0:
                break

            if lot.qty > 0 and remaining < 0:
                pnl = (f.price - lot.price) * close_qty
            elif lot.qty < 0 and remaining > 0:
                pnl = (lot.price - f.price) * close_qty
            else:
                pnl = 0.0

            agg.realized += pnl

            if abs(lot.qty) == close_qty:
                lots.pop(0)
            else:
                lot.qty += close_qty * (-1 if lot.qty > 0 else 1)

            remaining += close_qty if remaining < 0 else -close_qty

        if remaining != 0:
            lots.append(Lot(qty=remaining, price=f.price))

    def process_fills(self, fills: Iterable[Fill]) -> None:
        for f in sorted(fills, key=lambda x: x.ts):
            self.process_fill(f)

    def snapshot(
        self,
        price_lookup: Optional[Callable[[str], float]] = None
    ) -> Dict[str, Dict[str, Dict[str, float]]]:
        per_symbol: Dict[str, Aggregate] = {}
        per_strategy: Dict[str, Aggregate] = {}
        per_pair: Dict[Tuple[str, str], Aggregate] = {}

        def add_to(d: Dict[str, Aggregate], key: str, src: Aggregate):
            tgt = d.setdefault(key, Aggregate())
            tgt.realized += src.realized
            tgt.fees += src.fees

        def add_pair(key: Tuple[str, str], src: Aggregate):
            tgt = per_pair.setdefault(key, Aggregate())
            tgt.realized += src.realized
            tgt.fees += src.fees

        for (symbol, strat), agg in self._agg.items():
            add_to(per_symbol, symbol, agg)
            add_to(per_strategy, strat, agg)
            add_pair((symbol, strat), agg)

        unreal_per_pair: Dict[Tuple[str, str], float] = {}
        if price_lookup:
            for (symbol, strat), lots in self._lots.items():
                if not lots:
                    continue
                px = float(price_lookup(symbol) or 0.0)
                if px <= 0:
                    continue
                u = 0.0
                for lot in lots:
                    if lot.qty > 0:
                        u += (px - lot.price) * abs(lot.qty)
                    elif lot.qty < 0:
                        u += (lot.price - px) * abs(lot.qty)
                if u != 0.0:
                    unreal_per_pair[(symbol, strat)] = unreal_per_pair.get((symbol, strat), 0.0) + u

        out_per_symbol: Dict[str, Dict[str, float]] = {}
        for symbol, agg in per_symbol.items():
            unreal = 0.0
            for (sym, strat), u in unreal_per_pair.items():
                if sym == symbol:
                    unreal += u
            out_per_symbol[symbol] = {
                "realized": agg.realized,
                "unrealized": unreal,
                "fees": agg.fees,
                "equity": agg.equity(unreal),
            }

        out_per_strategy: Dict[str, Dict[str, float]] = {}
        for strat, agg in per_strategy.items():
            unreal = 0.0
            for (sym, s), u in unreal_per_pair.items():
                if s == strat:
                    unreal += u
            out_per_strategy[strat] = {
                "realized": agg.realized,
                "unrealized": unreal,
                "fees": agg.fees,
                "equity": agg.equity(unreal),
            }

        out_per_pair: Dict[str, Dict[str, float]] = {}
        for (symbol, strat), agg in per_pair.items():
            u = unreal_per_pair.get((symbol, strat), 0.0)
            key = f"{strat}:{symbol}"
            out_per_pair[key] = {
                "symbol": symbol,
                "strategy": strat,
                "realized": agg.realized,
                "unrealized": u,
                "fees": agg.fees,
                "equity": agg.equity(u),
            }

        total_realized = sum(a.realized for a in self._agg.values())
        total_fees = sum(a.fees for a in self._agg.values())
        total_unreal = sum(unreal_per_pair.values())
        total = {
            "realized": total_realized,
            "unrealized": total_unreal,
            "fees": total_fees,
            "equity": total_realized + total_unreal - total_fees,
        }

        return {
            "total": total,
            "per_symbol": out_per_symbol,
            "per_strategy": out_per_strategy,
            "per_pair": out_per_pair,
        }
